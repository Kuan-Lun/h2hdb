"""Fake HTTP responses prove the opt-in probe contract without opening sockets."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.request import Request

import pytest


def _load_module() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts/deployment_acceptance/http_probe.py"
    )
    spec = importlib.util.spec_from_file_location("acceptance_http_probe_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


probe = _load_module()
BASE = "http://127.0.0.1:8000"
GID = 1_000_001
CONTENT = bytes(range(256)) * 1024
DIGEST = sha256(CONTENT).hexdigest()


class Response:
    def __init__(self, body: bytes, status: int = 200, /, **headers: str) -> None:
        self.status = status
        self.headers = headers
        self.body = io.BytesIO(body)
        self.read_sizes: list[int] = []
        self.read_bytes = 0
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        assert size > 0, "The probe must not request an unbounded read"
        self.read_sizes.append(size)
        result = self.body.read(size)
        self.read_bytes += len(result)
        return result

    def close(self) -> None:
        self.closed = True


class Opener:
    def __init__(self, responses: list[Response]) -> None:
        self.responses = responses
        self.requests: list[Request] = []

    def open(self, request: Request, *, timeout: float) -> Response:
        assert timeout == 60.0
        assert request.get_header("Accept-encoding") == "identity"
        self.requests.append(request)
        return self.responses[len(self.requests) - 1]


def _document(
    *, href: object = "http://h2hdb-opds:8000/opds/v2/acquisitions/a?revision=7"
) -> dict[str, Any]:
    return {
        "publications": [
            {
                "metadata": {"identifier": f"urn:h2h:gallery:{GID}"},
                "links": [
                    {"rel": "self", "href": "/publication"},
                    {
                        "rel": "http://opds-spec.org/acquisition/open-access",
                        "href": href,
                        "size": len(CONTENT),
                    },
                ],
            }
        ]
    }


def _responses(
    document: object | None = None, *, content: bytes = CONTENT
) -> list[Response]:
    return [
        Response(json.dumps(_document() if document is None else document).encode()),
        Response(content, **{"Content-Length": str(len(CONTENT))}),
        Response(
            content[:32],
            206,
            **{"Content-Range": f"bytes 0-31/{len(CONTENT)}", "Content-Length": "32"},
        ),
    ]


def _install(monkeypatch: pytest.MonkeyPatch, responses: list[Response]) -> Opener:
    opener = Opener(responses)

    def build(*handlers: object) -> Opener:
        assert len(handlers) == 2
        assert isinstance(handlers[0], probe.ProxyHandler)
        assert handlers[0].proxies == {}
        assert isinstance(handlers[1], probe._NoRedirect)
        return opener

    monkeypatch.setattr(probe, "build_opener", build)
    return opener


def test_search_download_and_range_verify_actual_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _responses()
    opener = _install(monkeypatch, responses)
    result = probe.probe(BASE, GID, DIGEST, len(CONTENT))
    assert result["status"] == "passed"
    assert result["gid"] == GID
    assert result["byte_length"] == len(CONTENT)
    assert result["sha256"] == DIGEST
    assert result["range_verified"] is True and result["range_bytes"] == 32
    assert "content" not in result and "prefix" not in result
    assert all(result[name] >= 0 for name in result if name.endswith("_seconds"))
    assert [request.full_url for request in opener.requests] == [
        BASE + f"/opds/v2/search?query={GID}&limit=2",
        BASE + "/opds/v2/acquisitions/a?revision=7",
        BASE + "/opds/v2/acquisitions/a?revision=7",
    ]
    assert opener.requests[2].get_header("Range") == "bytes=0-31"
    assert max(responses[1].read_sizes) <= 65536
    assert all(response.closed for response in responses)


@pytest.mark.parametrize(
    "href", ["/opds/v2/acquisitions/a", BASE + "/opds/v2/acquisitions/a"]
)
def test_relative_and_same_origin_links_are_accepted(
    monkeypatch: pytest.MonkeyPatch, href: str
) -> None:
    opener = _install(monkeypatch, _responses(_document(href=href)))
    result = probe.probe(BASE, GID, DIGEST, len(CONTENT), check_range=False)
    assert len(opener.requests) == 2
    assert result["range_verified"] is False and result["range_bytes"] == 0


@pytest.mark.parametrize(
    "href",
    [
        "https://example.com/archive",
        "http://example.com/archive",
        "//example.com/archive",
        "http://h2hdb-opds:8001/archive",
        "http://user:password@127.0.0.1:8000/archive",
        "file:///etc/passwd",
        "http://127.0.0.1:8000/archive#fragment",
        "\n/opds/v2/acquisitions/a",
        123,
    ],
)
def test_unsafe_advertised_origin_is_rejected_before_download(
    monkeypatch: pytest.MonkeyPatch, href: object
) -> None:
    opener = _install(monkeypatch, _responses(_document(href=href)))
    with pytest.raises(probe.ProbeError):
        probe.probe(BASE, GID, DIGEST, len(CONTENT))
    assert len(opener.requests) == 1


@pytest.mark.parametrize(
    "base,gid,digest,size,timeout",
    [
        ("http://example.com:8000", GID, DIGEST, len(CONTENT), 60.0),
        ("http://127.0.0.1:8000/path", GID, DIGEST, len(CONTENT), 60.0),
        ("http://127.0.0.1:8000?x=y", GID, DIGEST, len(CONTENT), 60.0),
        (BASE, 0, DIGEST, len(CONTENT), 60.0),
        (BASE, GID, "bad", len(CONTENT), 60.0),
        (BASE, GID, DIGEST, 0, 60.0),
        (BASE, GID, DIGEST, len(CONTENT), float("nan")),
        (BASE, GID, DIGEST, len(CONTENT), float("inf")),
        (BASE, GID, DIGEST, len(CONTENT), 0.0),
    ],
)
def test_invalid_arguments_do_not_create_an_http_client(
    monkeypatch: pytest.MonkeyPatch,
    base: str,
    gid: int,
    digest: str,
    size: int,
    timeout: float,
) -> None:
    def forbidden(*_handlers: object) -> None:
        raise AssertionError("Invalid request created an HTTP client")

    monkeypatch.setattr(probe, "build_opener", forbidden)
    with pytest.raises(probe.ProbeError):
        probe.probe(base, gid, digest, size, timeout=timeout)


@pytest.mark.parametrize(
    "change", ["empty", "multiple", "gid", "no-acquisition", "duplicate", "size"]
)
def test_search_requires_exact_identity_one_acquisition_and_advertised_size(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    document = _document()
    publication = document["publications"][0]
    if change == "empty":
        document["publications"] = []
    elif change == "multiple":
        document["publications"].append(publication)
    elif change == "gid":
        publication["metadata"]["identifier"] = "urn:h2h:gallery:7"
    elif change == "no-acquisition":
        publication["links"].pop()
    elif change == "duplicate":
        publication["links"].append(publication["links"][-1])
    else:
        publication["links"][-1]["size"] += 1
    opener = _install(monkeypatch, _responses(document))
    with pytest.raises(probe.ProbeError):
        probe.probe(BASE, GID, DIGEST, len(CONTENT))
    assert len(opener.requests) == 1


@pytest.mark.parametrize(
    "content", [CONTENT[:-1], CONTENT + b"overflow", b"x" * len(CONTENT)]
)
def test_download_rejects_truncation_overflow_and_digest_mismatch(
    monkeypatch: pytest.MonkeyPatch, content: bytes
) -> None:
    responses = _responses(content=content)
    opener = _install(monkeypatch, responses)
    with pytest.raises(probe.ProbeError):
        probe.probe(BASE, GID, DIGEST, len(CONTENT))
    assert len(opener.requests) == 2
    assert responses[1].read_bytes <= len(CONTENT) + 1
    assert responses[1].closed


@pytest.mark.parametrize(
    "change", ["status", "extent", "length", "content", "overflow"]
)
def test_range_checks_status_headers_and_exact_prefix(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    responses = _responses()
    response = responses[-1]
    if change == "status":
        response.status = 200
    elif change == "extent":
        response.headers["Content-Range"] = f"bytes 1-32/{len(CONTENT)}"
    elif change == "length":
        response.headers["Content-Length"] = "31"
    else:
        response.body = io.BytesIO(b"x" * 32 if change == "content" else CONTENT[:33])
    _install(monkeypatch, responses)
    with pytest.raises(probe.ProbeError):
        probe.probe(BASE, GID, DIGEST, len(CONTENT))
    assert response.closed


def test_short_artifact_ranges_only_existing_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = b"abc"
    document = _document()
    document["publications"][0]["links"][-1]["size"] = 3
    responses = [
        Response(json.dumps(document).encode()),
        Response(content, **{"Content-Length": "3"}),
        Response(
            content, 206, **{"Content-Length": "3", "Content-Range": "bytes 0-2/3"}
        ),
    ]
    opener = _install(monkeypatch, responses)
    result = probe.probe(BASE, GID, sha256(content).hexdigest(), 3)
    assert result["range_bytes"] == 3
    assert opener.requests[-1].get_header("Range") == "bytes=0-2"


def test_search_body_and_encoded_transfer_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = _responses()
    responses[0].body = io.BytesIO(b" " * (1024 * 1024 + 100))
    _install(monkeypatch, responses)
    with pytest.raises(probe.ProbeError, match="byte count"):
        probe.probe(BASE, GID, DIGEST, len(CONTENT))
    assert responses[0].read_bytes == 1024 * 1024 + 1
    responses = _responses()
    responses[1].headers["Content-Encoding"] = "gzip"
    _install(monkeypatch, responses)
    with pytest.raises(probe.ProbeError, match="Encoded"):
        probe.probe(BASE, GID, DIGEST, len(CONTENT))
    assert responses[1].closed and responses[1].read_bytes == 0


def test_redirect_handler_never_follows_even_local_redirect() -> None:
    with pytest.raises(probe.ProbeError, match="Redirect"):
        probe._NoRedirect().redirect_request(None, None, 302, "redirect", None, BASE)


def test_cli_prints_only_result_and_returns_failure_for_bad_bytes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    arguments = [
        "--base-url",
        BASE,
        "--gid",
        str(GID),
        "--sha256",
        DIGEST,
        "--size",
        str(len(CONTENT)),
    ]
    _install(monkeypatch, _responses())
    assert probe.main(arguments) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["sha256"] == DIGEST
    assert output.err == ""
    _install(monkeypatch, _responses(content=b"x" * len(CONTENT)))
    assert probe.main(arguments) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err)["status"] == "failed"
