"""Validate the bounded admission limit used while freezing complete galleries."""

from __future__ import annotations

MAX_NEW_GALLERIES = 1_000_000


def require_source_batch_limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= MAX_NEW_GALLERIES:
        raise ValueError(
            f"max_new_galleries must be an integer in 1..{MAX_NEW_GALLERIES}"
        )
    return value
