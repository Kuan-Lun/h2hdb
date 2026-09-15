"""Verify actual OPDS acquisition bytes on an explicitly supplied loopback server.

This opt-in tool never uses environment proxies, follows redirects, or writes
downloaded content. Only the documented library-activation HTTP 503 contract
permits another request, with every wait sharing one probe budget. The caller
must also bound the process lifetime: the budget checks I/O boundaries and
cannot interrupt a socket read kept alive by a continuously trickling peer.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import math
import re
import sys
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass, field
from email.message import Message
from hashlib import sha256 as sha256_digest
from time import perf_counter, sleep
from typing import IO, Any, Protocol, cast
from urllib.error import HTTPError
from urllib.parse import SplitResult, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import (
    HTTPRedirectHandler,
    OpenerDirector,
    ProxyHandler,
    Request,
    build_opener,
)

_JSON_LIMIT = 1024 * 1024
_CHUNK_BYTES = 64 * 1024
_MAINTENANCE_BODY_LIMIT = 4096
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ACQUISITION = re.compile(r"(?:^|/)acquisition(?:/open-access)?\Z")


class ProbeError(ValueError):
    """The HTTP response did not satisfy the expected publication contract."""

    def __init__(
        self, message: str, *, maintenance_events: list[dict[str, object]] | None = None
    ) -> None:
        super().__init__(message)
        self.maintenance_events = maintenance_events or []


@dataclass
class _ProbeBudget:
    socket_timeout: float
    duration: float
    started: float
    maintenance_events: list[dict[str, object]] = field(default_factory=list)

    def remaining(self) -> float:
        remaining = self.duration - (perf_counter() - self.started)
        if remaining <= 0:
            raise ProbeError("HTTP probe deadline reached")
        return remaining

    def timeout(self) -> float:
        return min(self.socket_timeout, self.remaining())

    def wait_for_maintenance(self, phase: str) -> None:
        event: dict[str, object] = {
            "phase": phase,
            "status": 503,
            "code": "library_activating",
            "retry_after_seconds": 1,
            "elapsed_seconds": perf_counter() - self.started,
            "waited_seconds": 0.0,
        }
        self.maintenance_events.append(event)
        if self.remaining() <= 1:
            raise ProbeError("HTTP probe deadline cannot accommodate Retry-After")
        started = perf_counter()
        sleep(1)
        event["waited_seconds"] = perf_counter() - started
        self.remaining()


class _Response(Protocol):
    status: int
    headers: Mapping[str, str]

    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> None:
        raise ProbeError("Redirects are not permitted by the local HTTP probe")


def _url(value: str) -> SplitResult:
    if not value or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise ProbeError("HTTP URL is empty or contains control characters")
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as error:
        raise ProbeError("HTTP URL has an invalid port") from error
    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or (port is not None and port <= 0)
        or "\\" in parsed.netloc
    ):
        raise ProbeError(
            "HTTP URL must be an uncredentialed HTTP origin without fragments"
        )
    return parsed


def _origin(value: SplitResult) -> tuple[str, str | None, int]:
    return value.scheme, value.hostname, value.port or 80


def _base(value: str) -> SplitResult:
    parsed = _url(value)
    host = parsed.hostname
    try:
        loopback = host is not None and ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = host == "localhost"
    if not loopback or parsed.path not in {"", "/"} or parsed.query:
        raise ProbeError(
            "Base URL must be a loopback HTTP origin without a path or query"
        )
    return parsed._replace(path="", query="", fragment="")


def _acquisition_url(base: SplitResult, href: object) -> str:
    if not isinstance(href, str):
        raise ProbeError("Acquisition link requires a string href")
    if not href or any(
        ord(character) < 32 or ord(character) == 127 for character in href
    ):
        raise ProbeError("Acquisition href is empty or contains control characters")
    advertised = _url(urljoin(urlunsplit(base) + "/", href))
    allowed = {_origin(base), ("http", "h2hdb-opds", 8000)}
    if _origin(advertised) not in allowed:
        raise ProbeError("Acquisition link origin is outside the isolated OPDS service")
    # Preserve the revision query and resource path, but never resolve the
    # advertised container hostname from the host running this probe.
    return urlunsplit(advertised._replace(scheme=base.scheme, netloc=base.netloc))


def _request(
    opener: OpenerDirector,
    url: str,
    *,
    budget: _ProbeBudget,
    phase: str,
    byte_range: str | None = None,
) -> _Response:
    headers = {"Accept-Encoding": "identity"}
    if byte_range is not None:
        headers["Range"] = byte_range
    while True:
        try:
            response = cast(
                _Response,
                opener.open(Request(url, headers=headers), timeout=budget.timeout()),
            )
        except HTTPError as error:
            with closing(error):
                if error.code != 503:
                    raise
                _require_maintenance_response(error, budget)
            budget.wait_for_maintenance(phase)
            continue
        try:
            budget.remaining()
            if (
                response.headers.get("Content-Encoding", "identity").lower()
                != "identity"
            ):
                raise ProbeError("Encoded transfer would hide the advertised CBZ bytes")
        except BaseException:
            response.close()
            raise
        return response


def _require_maintenance_response(error: HTTPError, budget: _ProbeBudget) -> None:
    if (
        error.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        != "application/json"
        or error.headers.get("Retry-After") != "1"
        or error.headers.get("Cache-Control") != "no-store"
        or error.headers.get("Content-Encoding", "identity").lower() != "identity"
    ):
        raise ProbeError("HTTP 503 does not satisfy the library-activation headers")
    try:
        document = json.loads(
            _bounded_read(cast(_Response, error), _MAINTENANCE_BODY_LIMIT, budget)
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as invalid:
        raise ProbeError("HTTP 503 has an invalid library-activation body") from invalid
    if (
        not isinstance(document, dict)
        or set(document) != {"code", "detail"}
        or document["code"] != "library_activating"
        or not isinstance(document["detail"], str)
        or not document["detail"].strip()
    ):
        raise ProbeError("HTTP 503 is not the documented library-activation response")


def _bounded_read(response: _Response, limit: int, budget: _ProbeBudget) -> bytes:
    chunks: list[bytes] = []
    count = 0
    while count <= limit:
        budget.remaining()
        value = response.read(min(_CHUNK_BYTES, limit + 1 - count))
        budget.remaining()
        if not value:
            return b"".join(chunks)
        count += len(value)
        if count > limit:
            raise ProbeError("HTTP response exceeded its allowed byte count")
        chunks.append(value)
    raise AssertionError("Bounded read did not terminate")


def _link(document: object, gid: int, size: int) -> Mapping[str, Any]:
    if not isinstance(document, dict):
        raise ProbeError("Search response must be a JSON object")
    publications = document.get("publications")
    if not isinstance(publications, list) or len(publications) != 1:
        raise ProbeError("Exact GID search must return exactly one publication")
    publication = publications[0]
    if not isinstance(publication, dict) or not isinstance(
        publication.get("metadata"), dict
    ):
        raise ProbeError("Search publication has no metadata")
    if publication["metadata"].get("identifier") != f"urn:h2h:gallery:{gid}":
        raise ProbeError("Search publication identifier differs from the requested GID")
    links = publication.get("links")
    if not isinstance(links, list) or any(not isinstance(link, dict) for link in links):
        raise ProbeError("Publication links must be objects")
    acquisitions = [
        link
        for link in links
        if isinstance(link.get("rel"), str) and _ACQUISITION.search(link["rel"])
    ]
    if len(acquisitions) != 1:
        raise ProbeError("Publication must advertise exactly one acquisition")
    result: Mapping[str, Any] = acquisitions[0]
    if type(result.get("size")) is not int or result["size"] != size:
        raise ProbeError("Advertised acquisition size differs from the expected CBZ")
    return result


def probe(
    base_url: str,
    gid: int,
    sha256: str,
    size: int,
    *,
    check_range: bool = True,
    timeout: float = 60.0,
    deadline_seconds: float = 60.0,
) -> dict[str, object]:
    """Search, stream-verify one CBZ, and optionally verify its initial range."""
    base = _base(base_url)
    if type(gid) is not int or not 1 <= gid < 2**63:
        raise ProbeError("GID must be a positive int63")
    if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
        raise ProbeError("Expected SHA-256 must be 64 lowercase hexadecimal characters")
    if type(size) is not int or not 1 <= size < 2**63:
        raise ProbeError("Expected CBZ size must be a positive int63")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ProbeError("HTTP socket timeout must be finite and positive")
    if not math.isfinite(deadline_seconds) or deadline_seconds <= 0:
        raise ProbeError("HTTP probe deadline must be finite and positive")
    budget = _ProbeBudget(timeout, deadline_seconds, perf_counter())
    try:
        return _verify(base, gid, sha256, size, check_range=check_range, budget=budget)
    except Exception as error:
        if not budget.maintenance_events:
            raise
        raise ProbeError(
            f"{type(error).__name__}: {error}",
            maintenance_events=budget.maintenance_events,
        ) from error


def _verify(
    base: SplitResult,
    gid: int,
    sha256: str,
    size: int,
    *,
    check_range: bool,
    budget: _ProbeBudget,
) -> dict[str, object]:
    opener = build_opener(ProxyHandler({}), _NoRedirect())
    started = budget.started
    search_url = (
        urlunsplit(base)
        + "/opds/v2/search?"
        + urlencode({"query": f"gid:{gid}", "limit": 2})
    )
    with closing(
        _request(opener, search_url, budget=budget, phase="search")
    ) as response:
        if response.status != 200:
            raise ProbeError("OPDS search did not return HTTP 200")
        document = json.loads(_bounded_read(response, _JSON_LIMIT, budget))
    acquisition = _link(document, gid, size)
    acquisition_url = _acquisition_url(base, acquisition.get("href"))
    search_seconds = perf_counter() - started

    download_started = perf_counter()
    digest = sha256_digest()
    count = 0
    prefix = bytearray()
    with closing(
        _request(opener, acquisition_url, budget=budget, phase="download")
    ) as response:
        if response.status != 200:
            raise ProbeError("CBZ acquisition did not return HTTP 200")
        if response.headers.get("Content-Length") != str(size):
            raise ProbeError("CBZ Content-Length differs from its expected size")
        while count <= size:
            budget.remaining()
            value = response.read(min(_CHUNK_BYTES, size + 1 - count))
            budget.remaining()
            if not value:
                break
            count += len(value)
            if count > size:
                raise ProbeError("CBZ response exceeded its expected size")
            digest.update(value)
            prefix.extend(value[: max(0, 32 - len(prefix))])
    if count != size or digest.hexdigest() != sha256:
        raise ProbeError(
            "Downloaded CBZ size or SHA-256 differs from the expected artifact"
        )
    download_seconds = perf_counter() - download_started

    range_seconds = 0.0
    range_size = 0
    if check_range:
        range_started = perf_counter()
        end = min(31, size - 1)
        with closing(
            _request(
                opener,
                acquisition_url,
                budget=budget,
                phase="range",
                byte_range=f"bytes=0-{end}",
            )
        ) as response:
            if (
                response.status != 206
                or response.headers.get("Content-Range") != f"bytes 0-{end}/{size}"
                or response.headers.get("Content-Length") != str(end + 1)
            ):
                raise ProbeError(
                    "CBZ range response has incorrect status or extent headers"
                )
            selected = _bounded_read(response, end + 1, budget)
            if selected != bytes(prefix):
                raise ProbeError(
                    "CBZ range response differs from the complete download prefix"
                )
            range_size = len(selected)
        range_seconds = perf_counter() - range_started
    return {
        "status": "passed",
        "gid": gid,
        "publications": 1,
        "acquisitions": 1,
        "byte_length": count,
        "sha256": digest.hexdigest(),
        "range_verified": check_range,
        "range_bytes": range_size,
        "search_seconds": search_seconds,
        "download_seconds": download_seconds,
        "range_seconds": range_seconds,
        "total_seconds": perf_counter() - started,
        "deadline_seconds": budget.duration,
        "maintenance_events": budget.maintenance_events,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--gid", required=True, type=int)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--size", required=True, type=int)
    parser.add_argument("--no-range", action="store_true")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--deadline-seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    try:
        result = probe(
            args.base_url,
            args.gid,
            args.sha256,
            args.size,
            check_range=not args.no_range,
            timeout=args.timeout,
            deadline_seconds=args.deadline_seconds,
        )
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": f"{type(error).__name__}: {error}",
                    "maintenance_events": (
                        error.maintenance_events
                        if isinstance(error, ProbeError)
                        else []
                    ),
                }
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
