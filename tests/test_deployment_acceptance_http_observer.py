"""Observer state-machine contracts; actual HTTP runs belong to opt-in Compose."""

from __future__ import annotations

import importlib
import io
import json
from email.message import Message
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.error import HTTPError

import pytest

BASE = "http://127.0.0.1:8000"
OLD = b"original synthetic CBZ"
NEW = b"replacement synthetic CBZ"


@pytest.fixture
def observer(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    return importlib.import_module("deployment_acceptance.http_observer")


class Response:
    def __init__(self, body: bytes, **headers: str) -> None:
        self.status = 200
        self.headers = headers
        self.body = io.BytesIO(body)
        self.closed = False

    def read(self, size: int) -> bytes:
        assert 0 < size <= 65536
        return self.body.read(size)

    def close(self) -> None:
        self.closed = True


def search(revision: int, content: bytes) -> Response:
    return Response(
        json.dumps(
            {
                "links": [
                    {
                        "rel": "self",
                        "href": f"{BASE}/opds/v2/search?query=gid%3A1&limit=2&revision={revision}",
                    }
                ],
                "publications": [
                    {
                        "metadata": {
                            "identifier": "urn:h2h:gallery:1",
                            "title": f"Generation {revision}",
                        },
                        "links": [
                            {
                                "rel": "http://opds-spec.org/acquisition/open-access",
                                "href": f"{BASE}/opds/v2/acquisitions/a?revision={revision}",
                                "size": len(content),
                            }
                        ],
                    }
                ],
            }
        ).encode()
    )


def archive(content: bytes) -> Response:
    return Response(
        content,
        **{
            "Content-Length": str(len(content)),
            "ETag": f'"{sha256(content).hexdigest()}"',
        },
    )


def error(status: int, body: object = None, **headers: str) -> HTTPError:
    message = Message()
    for key, value in headers.items():
        message[key] = value
    return HTTPError(
        BASE, status, "test", message, io.BytesIO(json.dumps(body).encode())
    )


def redirect(**headers: str) -> HTTPError:
    return error(
        303,
        **{
            "Location": BASE + "/opds/v2/search?query=gid%3A1&limit=2",
            "Cache-Control": "no-store",
            **headers,
        },
    )


def install(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    responses: list[Response | HTTPError],
) -> list[str]:
    requests = []

    class Opener:
        def open(self, request: Any, *, timeout: float) -> Response:
            assert 0 < timeout <= 5
            requests.append(request.full_url)
            selected = responses.pop(0)
            if isinstance(selected, HTTPError):
                raise selected
            return selected

    monkeypatch.setattr(module, "build_opener", lambda *_handlers: Opener())
    return requests


def final_file(path: Path, *, content: bytes = NEW, revision: int = 8) -> None:
    path.write_text(
        json.dumps(
            {
                "gid": 1,
                "revision": revision,
                "sha256": sha256(content).hexdigest(),
                "byte_length": len(content),
            }
        )
    )


def test_hold_old_response_then_verify_new_bytes_and_explicit_stale_refresh(
    observer: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    held = archive(OLD)
    responses: list[Response | HTTPError] = [
        search(7, OLD),
        held,
        search(7, OLD),
        archive(OLD),
        search(7, OLD),
        archive(OLD),
        search(8, NEW),
        archive(NEW),
        search(8, NEW),
        archive(NEW),
        redirect(),
        search(8, NEW),
        archive(NEW),
    ]
    all_responses = list(responses)
    requests = install(monkeypatch, observer, responses)
    (tmp_path / "stream-ready-case.json").write_text("{}")

    def finish(_seconds: float) -> None:
        assert held.body.tell() == 0, (
            "Held download was drained before cleanup declaration"
        )
        assert (
            json.loads((tmp_path / "http-ready-case.json").read_text())["revision"] == 7
        )
        final_file(tmp_path / "finish-case.json")

    monkeypatch.setattr(observer, "sleep", finish)
    instance = observer.Observer(
        BASE,
        observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD)),
        deadline_seconds=60,
    )
    result = instance.observe(control=tmp_path, evidence=tmp_path, token="case")
    assert result["status"] == "passed"
    assert result["held_download_sha256"] == sha256(OLD).hexdigest()
    assert result["new_download_sha256"] == sha256(NEW).hexdigest()
    assert result["stale_navigation"] == {
        "status": 303,
        "refreshed_revision": 8,
        "query_preserved": True,
    }
    assert len(result["samples"]) == 5
    assert len(requests) == 13 and not responses
    assert all(
        response.closed for response in all_responses if isinstance(response, Response)
    )


def test_no_second_download_is_issued_before_server_gate_receipt(
    observer: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    held = archive(OLD)
    requests = install(monkeypatch, observer, [search(7, OLD), held])
    instance = observer.Observer(
        BASE,
        observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD)),
        deadline_seconds=60,
    )

    def gate_absent(_seconds: float) -> None:
        assert len(requests) == 2 and held.body.tell() == 0
        assert not (tmp_path / "http-ready-case.json").exists()
        raise TimeoutError("gate never arrived")

    monkeypatch.setattr(observer, "sleep", gate_absent)
    with pytest.raises(TimeoutError, match="gate never arrived"):
        instance.observe(control=tmp_path, evidence=tmp_path, token="case")
    assert held.closed


@pytest.mark.parametrize(
    "status,detail",
    [
        (404, "Catalog revision 7 not found"),
        (500, "failure"),
        (404, "Resource is unavailable"),
    ],
)
def test_only_exact_stale_acquisition_race_is_accepted(
    observer: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    detail: str,
) -> None:
    install(
        monkeypatch,
        observer,
        [
            search(7, OLD),
            error(status, {"detail": detail}, **{"Cache-Control": "no-store"}),
        ],
    )
    instance = observer.Observer(
        BASE,
        observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD)),
        deadline_seconds=60,
    )
    if status == 404 and detail == "Catalog revision 7 not found":
        assert instance.sample()["status"] == "revision_changed_before_download"
    else:
        with pytest.raises((HTTPError, observer.http.ProbeError)):
            instance.sample()


def test_documented_activation_503_is_retried_and_recorded(
    observer: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    maintenance = error(
        503,
        {"code": "library_activating", "detail": "publishing"},
        **{
            "Content-Type": "application/json",
            "Retry-After": "1",
            "Cache-Control": "no-store",
        },
    )
    requests = install(
        monkeypatch, observer, [maintenance, search(7, OLD), archive(OLD)]
    )
    waits: list[float] = []
    monkeypatch.setattr(observer.http, "sleep", waits.append)
    instance = observer.Observer(
        BASE,
        observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD)),
        deadline_seconds=60,
    )
    assert instance.sample()["status"] == "verified"
    assert requests[0] == requests[1] and waits == [1]
    assert instance.budget.maintenance_events[0]["phase"] == "observer-search"


@pytest.mark.parametrize("change", ["bytes", "etag", "size"])
def test_archive_mismatch_never_becomes_success(
    observer: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    response = archive(OLD)
    if change == "bytes":
        response.body = io.BytesIO(b"x" * len(OLD))
    elif change == "etag":
        response.headers["ETag"] = '"invalid"'
    else:
        response.headers["Content-Length"] = "100"
    install(monkeypatch, observer, [search(7, OLD), response])
    instance = observer.Observer(
        BASE,
        observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD)),
        deadline_seconds=60,
    )
    with pytest.raises(observer.http.ProbeError):
        instance.sample()
    assert response.closed


@pytest.mark.parametrize(
    "location",
    [
        "http://example.com/opds/v2/search?query=gid%3A1&limit=2",
        BASE + "/opds/v2/search?query=gid%3A2&limit=2",
        BASE + "/opds/v2/search?query=gid%3A1&limit=2&revision=7",
        BASE + "/opds/v2/publications?query=gid%3A1&limit=2",
    ],
)
def test_stale_redirect_cannot_change_origin_query_or_keep_old_revision(
    observer: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    location: str,
) -> None:
    requests = install(monkeypatch, observer, [redirect(Location=location)])
    instance = observer.Observer(
        BASE,
        observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD)),
        deadline_seconds=60,
    )
    with pytest.raises(observer.http.ProbeError):
        instance.stale_navigation(
            BASE + "/old", observer.Artifact(1, 8, sha256(NEW).hexdigest(), len(NEW))
        )
    assert len(requests) == 1


def test_final_oracle_mismatch_fails_even_if_etag_matches_bytes(
    observer: ModuleType,
) -> None:
    with pytest.raises(observer.http.ProbeError, match="independent oracle"):
        observer._require_sample(
            {
                "status": "verified",
                "revision": 8,
                "sha256": sha256(OLD).hexdigest(),
                "byte_length": len(OLD),
            },
            observer.Artifact(1, 8, sha256(NEW).hexdigest(), len(NEW)),
        )


@pytest.mark.parametrize(
    "change", ["new-under-old", "old-under-new", "backward", "outside"]
)
def test_history_binds_each_endpoint_revision_to_its_own_archive(
    observer: ModuleType,
    change: str,
) -> None:
    old = observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD))
    final = observer.Artifact(1, 8, sha256(NEW).hexdigest(), len(NEW))
    samples = [
        {
            "status": "verified",
            "revision": 7,
            "sha256": old.sha256,
            "byte_length": old.byte_length,
        },
        {
            "status": "verified",
            "revision": 8,
            "sha256": final.sha256,
            "byte_length": final.byte_length,
        },
    ]
    if change == "new-under-old":
        samples[0].update(sha256=final.sha256, byte_length=final.byte_length)
    elif change == "old-under-new":
        samples[1].update(sha256=old.sha256, byte_length=old.byte_length)
    elif change == "backward":
        samples.reverse()
    else:
        samples.insert(0, {"status": "revision_changed_before_download", "revision": 6})
    with pytest.raises(observer.http.ProbeError):
        observer._require_sample_history(samples, old, final)


def test_cli_writes_failure_evidence_and_stops_its_watchdog(
    observer: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def failed(_self: object, **_kwargs: object) -> None:
        raise observer.http.ProbeError("synthetic failed assertion")

    monkeypatch.setattr(observer.Observer, "observe", failed)
    assert (
        observer.main(
            [
                "--base-url",
                BASE,
                "--gid",
                "1",
                "--revision",
                "7",
                "--sha256",
                sha256(OLD).hexdigest(),
                "--size",
                str(len(OLD)),
                "--token",
                "case",
                "--control-directory",
                str(tmp_path),
                "--evidence-directory",
                str(tmp_path),
                "--deadline-seconds",
                "2",
            ]
        )
        == 1
    )
    result = json.loads((tmp_path / "http-result-case.json").read_text())
    assert (
        result["status"] == "failed"
        and "synthetic failed assertion" in result["failure"]
    )
    assert json.loads(capsys.readouterr().err) == result


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), 3601])
def test_bad_deadline_never_builds_http_client(
    observer: ModuleType, monkeypatch: pytest.MonkeyPatch, value: float
) -> None:
    def forbidden(*_handlers: object) -> None:
        raise AssertionError("Invalid observer opened client")

    monkeypatch.setattr(observer, "build_opener", forbidden)
    with pytest.raises(observer.http.ProbeError, match="deadline"):
        observer.Observer(
            BASE,
            observer.Artifact(1, 7, sha256(OLD).hexdigest(), len(OLD)),
            deadline_seconds=value,
        )
