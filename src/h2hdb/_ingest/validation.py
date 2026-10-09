"""Ingest value checks; each caller retains its own fresh authority checks."""

from __future__ import annotations

from ..domain import VNextIngestPage, VNextIngestSession


def _session_authority_identity(session: VNextIngestSession) -> tuple[object, ...]:
    if not isinstance(session, VNextIngestSession):
        raise TypeError("session must be VNextIngestSession")
    session.__post_init__()
    return (
        session.gate_owner_token,
        session.gate_generation,
        session.gate_slot,
        session.ingest_generation,
        session.ingest_owner_token,
        session.download_generation,
        session.handoff_owner_token,
        session.handoff_kind,
        session.consumed_at,
    )


def require_same_session_authority(
    issued: VNextIngestSession,
    current: VNextIngestSession,
    *,
    step: str,
) -> None:
    if _session_authority_identity(issued) != _session_authority_identity(current):
        raise ValueError(f"{step} step belongs to another ingest session authority")


def require_named_page(
    page: VNextIngestPage[object],
    *,
    after: bytes | None,
    capacity: int,
    label: str,
) -> bytes | None:
    if not isinstance(page, VNextIngestPage):
        raise TypeError(f"{label} adapter must return VNextIngestPage")
    page.__post_init__()
    if not page.terminal and len(page.items) != capacity:
        raise ValueError(f"nonterminal {label} page must contain {capacity} items")
    if page.terminal and not page.items and after is not None:
        raise ValueError(f"nonempty {label} streams cannot end with an empty page")
    prior = after
    for item in page.items:
        name = getattr(item, "name_bytes", None)
        if not isinstance(name, bytes):
            raise TypeError(f"{label} observation must expose bytes name_bytes")
        if prior is not None and name <= prior:
            raise ValueError(f"{label} page keys must be strictly increasing")
        prior = name
    if page.terminal:
        return None
    if not isinstance(page.next_after, bytes):
        raise TypeError(f"{label} next_after must be bytes")
    if not page.items or page.next_after != getattr(  # noqa: B009 - Adapter items expose named fields structurally.
        page.items[-1], "name_bytes"
    ):
        raise ValueError(f"{label} next_after must equal the last item key")
    return page.next_after


def require_tag_page(
    page: VNextIngestPage[object],
    *,
    after: int | None,
) -> int | None:
    if not isinstance(page, VNextIngestPage):
        raise TypeError("TAG adapter must return VNextIngestPage")
    page.__post_init__()
    if not page.terminal and len(page.items) != 256:
        raise ValueError("nonterminal TAG page must contain 256 items")
    if page.terminal and not page.items and after is not None:
        raise ValueError("nonempty TAG streams cannot end with an empty page")
    if page.terminal:
        return None
    if not isinstance(page.next_after, int):
        raise TypeError("TAG next_after must be an ordinal")
    start = 0 if after is None else after + 1
    if page.next_after != start + len(page.items) - 1:
        raise ValueError("TAG next_after must equal the last page ordinal")
    return page.next_after
