"""Tests for global deduplication reconciliation."""

import datetime
import hashlib
from collections.abc import Iterator
from itertools import permutations

import pytest

from h2hdb import H2HDB, H2HDBConfig
from h2hdb.gallery_deduplication import ContentClaim, select_reconciliation
from h2hdb.information import TagInformation


@pytest.fixture
def db(db_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=db_config)
    with instance:
        instance.create_main_tables()
        yield instance


def _make_gallery(
    db: H2HDB,
    name: str,
    *,
    title: str = "title",
    download_time: str = "2024-01-01 00:00:00",
    already_uploaded: bool = False,
) -> int:
    db.gallery_ids._insert_gallery_name(name)
    db_gallery_id = db.gallery_ids._get_db_gallery_id_by_gallery_name(name)
    db.gallery_titles._insert_gallery_title(db_gallery_id, title)
    db.gallery_times._insert_download_time(db_gallery_id, download_time)
    if already_uploaded:
        db.gallery_tags._insert_gallery_tags_many(
            {db_gallery_id: [TagInformation("misc", "already uploaded")]}
        )
    return db_gallery_id


def _sha256(payload: bytes) -> bytes:
    return hashlib.sha256(payload).digest()


def _claim(db: H2HDB, db_gallery_id: int, sha256: bytes | None) -> ContentClaim:
    return ContentClaim(
        db_gallery_id,
        sha256,
        db.gallery_deduplication._get_priority_key(db_gallery_id),
    )


def _dedup_state(
    db: H2HDB,
) -> tuple[dict[int, bytes], dict[int, int]]:
    return (
        db.gallery_deduplication._get_all_hashes("gallery_content_hashes"),
        db.gallery_deduplication._get_all_duplicate_warnings(),
    )


def test_reconcile_many_selects_higher_priority_independently_per_hash(
    db: H2HDB,
) -> None:
    weak_id = _make_gallery(db, "gallery-weak", title="short")
    strong_id = _make_gallery(db, "gallery-strong", title="a much longer title")
    independent_id = _make_gallery(db, "gallery-independent")
    hash_x = _sha256(b"content-x")
    hash_y = _sha256(b"content-y")
    claims = [
        _claim(db, weak_id, hash_x),
        _claim(db, independent_id, hash_y),
        _claim(db, strong_id, hash_x),
    ]

    result = db.gallery_deduplication.reconcile_many(claims)

    assert result.owner_hash_by_db_gallery_id == {
        strong_id: hash_x,
        independent_id: hash_y,
    }
    assert result.duplicate_of_by_db_gallery_id == {weak_id: strong_id}
    assert result.eligible_db_gallery_ids == frozenset({strong_id, independent_id})
    assert _dedup_state(db)[0] == result.owner_hash_by_db_gallery_id
    assert _dedup_state(db)[1] == result.duplicate_of_by_db_gallery_id


def test_reconcile_many_exact_tie_keeps_valid_incumbent(db: H2HDB) -> None:
    owner_id = _make_gallery(
        db, "gallery-owner", title="same", download_time="2024-01-01 00:00:00"
    )
    challenger_id = _make_gallery(
        db,
        "gallery-challenger",
        title="same",
        download_time="2024-01-01 00:00:00",
    )
    sha256 = _sha256(b"content")
    db.gallery_deduplication._claim_hash(owner_id, sha256)

    result = db.gallery_deduplication.reconcile_many(
        [
            _claim(db, challenger_id, sha256),
            _claim(db, owner_id, sha256),
        ]
    )

    assert result.owner_hash_by_db_gallery_id == {owner_id: sha256}
    assert result.duplicate_of_by_db_gallery_id == {challenger_id: owner_id}
    assert db.gallery_deduplication._get_hash_owner(sha256) == owner_id


def test_reconcile_many_exact_tie_without_incumbent_uses_max_gallery_id(
    db: H2HDB,
) -> None:
    first_id = _make_gallery(
        db, "gallery-first", title="same", download_time="2024-01-01 00:00:00"
    )
    second_id = _make_gallery(
        db,
        "gallery-second",
        title="same",
        download_time="2024-01-01 00:00:00",
    )
    sha256 = _sha256(b"content")
    winner_id = max(first_id, second_id)
    loser_id = min(first_id, second_id)

    result = db.gallery_deduplication.reconcile_many(
        [_claim(db, second_id, sha256), _claim(db, first_id, sha256)]
    )

    assert result.owner_hash_by_db_gallery_id == {winner_id: sha256}
    assert result.duplicate_of_by_db_gallery_id == {loser_id: winner_id}


def test_reconcile_many_ignores_owner_that_migrated_to_another_hash(
    db: H2HDB,
) -> None:
    gallery_a = _make_gallery(db, "gallery-a", title="a much longer title")
    gallery_b = _make_gallery(db, "gallery-b", title="short")
    hash_x = _sha256(b"content-x")
    hash_y = _sha256(b"content-y")
    db.gallery_deduplication._claim_hash(gallery_a, hash_x)

    result = db.gallery_deduplication.reconcile_many(
        [_claim(db, gallery_a, hash_y), _claim(db, gallery_b, hash_x)]
    )

    assert result.owner_hash_by_db_gallery_id == {
        gallery_a: hash_y,
        gallery_b: hash_x,
    }
    assert result.duplicate_of_by_db_gallery_id == {}
    assert result.eligible_db_gallery_ids == frozenset({gallery_a, gallery_b})
    assert _dedup_state(db)[0] == result.owner_hash_by_db_gallery_id


def test_reconcile_many_handles_cross_swap_of_existing_hashes(
    db: H2HDB,
) -> None:
    gallery_a = _make_gallery(db, "gallery-a")
    gallery_b = _make_gallery(db, "gallery-b")
    hash_x = _sha256(b"content-x")
    hash_y = _sha256(b"content-y")
    db.gallery_deduplication._claim_hash(gallery_a, hash_x)
    db.gallery_deduplication._claim_hash(gallery_b, hash_y)

    result = db.gallery_deduplication.reconcile_many(
        [_claim(db, gallery_a, hash_y), _claim(db, gallery_b, hash_x)]
    )

    assert result.owner_hash_by_db_gallery_id == {
        gallery_a: hash_y,
        gallery_b: hash_x,
    }
    assert result.duplicate_of_by_db_gallery_id == {}
    assert _dedup_state(db)[0] == result.owner_hash_by_db_gallery_id


def test_reconcile_many_contentless_gallery_loses_no_cbz_eligibility(
    db: H2HDB,
) -> None:
    contentless_id = _make_gallery(db, "gallery-contentless")
    other_id = _make_gallery(db, "gallery-other")
    old_hash = _sha256(b"old-content")
    other_hash = _sha256(b"other-content")
    db.gallery_deduplication._claim_hash(contentless_id, old_hash)
    db.gallery_deduplication._claim_hash(other_id, other_hash)
    db.gallery_deduplication._record_duplicate_warning(contentless_id, other_id)

    result = db.gallery_deduplication.reconcile_many(
        [
            _claim(db, contentless_id, None),
            _claim(db, other_id, other_hash),
        ]
    )

    assert result.owner_hash_by_db_gallery_id == {other_id: other_hash}
    assert result.duplicate_of_by_db_gallery_id == {}
    assert result.eligible_db_gallery_ids == frozenset({contentless_id, other_id})
    assert _dedup_state(db) == ({other_id: other_hash}, {})


def test_reconcile_many_retargets_stale_warning_to_final_winner(
    db: H2HDB,
) -> None:
    weak_id = _make_gallery(db, "gallery-weak", title="short")
    incumbent_id = _make_gallery(db, "gallery-incumbent", title="medium title")
    winner_id = _make_gallery(db, "gallery-winner", title="a much longer title indeed")
    sha256 = _sha256(b"content")
    db.gallery_deduplication._claim_hash(incumbent_id, sha256)
    db.gallery_deduplication._record_duplicate_warning(weak_id, incumbent_id)

    result = db.gallery_deduplication.reconcile_many(
        [
            _claim(db, weak_id, sha256),
            _claim(db, incumbent_id, sha256),
            _claim(db, winner_id, sha256),
        ]
    )

    assert result.owner_hash_by_db_gallery_id == {winner_id: sha256}
    assert result.duplicate_of_by_db_gallery_id == {
        weak_id: winner_id,
        incumbent_id: winner_id,
    }
    assert _dedup_state(db)[1] == result.duplicate_of_by_db_gallery_id


def test_reconcile_many_rejects_duplicate_claim_ids(db: H2HDB) -> None:
    gallery_id = _make_gallery(db, "gallery-a")
    claim = _claim(db, gallery_id, _sha256(b"content"))

    with pytest.raises(ValueError, match="exactly one content claim"):
        db.gallery_deduplication.reconcile_many([claim, claim])


def test_reconcile_many_is_idempotent(db: H2HDB) -> None:
    weak_id = _make_gallery(db, "gallery-weak", title="short")
    strong_id = _make_gallery(db, "gallery-strong", title="a much longer title")
    sha256 = _sha256(b"content")
    claims = [
        _claim(db, weak_id, sha256),
        _claim(db, strong_id, sha256),
    ]
    first_result = db.gallery_deduplication.reconcile_many(claims)
    first_state = _dedup_state(db)
    second_result = db.gallery_deduplication.reconcile_many(list(reversed(claims)))

    assert second_result == first_result
    assert _dedup_state(db) == first_state


def test_select_reconciliation_is_input_permutation_invariant() -> None:
    time = datetime.datetime(2024, 1, 1)
    hash_x = _sha256(b"content-x")
    hash_y = _sha256(b"content-y")
    claims = [
        ContentClaim(1, hash_x, (True, 10, time)),
        ContentClaim(2, hash_x, (True, 10, time)),
        ContentClaim(3, hash_y, (True, 20, time)),
        ContentClaim(4, hash_y, (True, 20, time)),
        ContentClaim(5, None, (False, 1, time)),
    ]
    existing_owners = {
        1: hash_x,
        999: _sha256(b"stale-content"),
    }
    expected = select_reconciliation(claims, existing_owners)

    for ordering in permutations(claims):
        assert select_reconciliation(ordering, existing_owners) == expected
