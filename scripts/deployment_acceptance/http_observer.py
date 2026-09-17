"""Observe real OPDS traffic from before an update until its cleanup completes.

Run in an isolated OPDS container, with probe variables unset. The runner writes
finish-TOKEN.json only after publication and cleanup evidence are confirmed.
The required server-side stream gate provides descriptor-lifetime evidence;
holding a client response alone does not establish that the server FD stayed open.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import threading
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import perf_counter, sleep
from typing import Any, cast
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from . import http_probe as http

_TOKEN = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


class _ManualRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        # urllib then exposes the 303 as HTTPError; the observer validates its
        # origin, status and exact refreshed query before making another request.
        return None


@dataclass(frozen=True)
class Artifact:
    gid: int
    revision: int
    sha256: str
    byte_length: int

    def __post_init__(self) -> None:
        if (
            any(
                type(value) is not int or not 0 < value < 2**63
                for value in (self.gid, self.revision, self.byte_length)
            )
            or not isinstance(self.sha256, str)
            or http._SHA256.fullmatch(self.sha256) is None
        ):
            raise http.ProbeError(
                "Observer artifact requires exact identity and digest"
            )

    @classmethod
    def read(cls, path: Path) -> Artifact:
        with path.open("rb") as stream:
            raw = stream.read(4097)
        if len(raw) > 4096:
            raise http.ProbeError("Observer finish declaration is too large")
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {
            "gid",
            "revision",
            "sha256",
            "byte_length",
        }:
            raise http.ProbeError("Observer finish declaration has an invalid shape")
        return cls(**value)


def _write(path: Path, value: object) -> None:
    with NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class Observer:
    def __init__(
        self, base_url: str, artifact: Artifact, *, deadline_seconds: float
    ) -> None:
        self.base = http._base(base_url)
        if not math.isfinite(deadline_seconds) or not 0 < deadline_seconds <= 3600:
            raise http.ProbeError(
                "Observer deadline must be positive and <=3600 seconds"
            )
        self.old = artifact
        self.budget = http._ProbeBudget(5, deadline_seconds, perf_counter())
        self.opener = build_opener(ProxyHandler({}), _ManualRedirect())
        self.search_url = (
            urlunsplit(self.base)
            + "/opds/v2/search?"
            + urlencode({"query": f"gid:{artifact.gid}", "limit": 2})
        )
        self.samples: list[dict[str, object]] = []

    def request(self, url: str, phase: str) -> http._Response:
        while True:
            try:
                response = cast(
                    http._Response,
                    self.opener.open(
                        Request(url, headers={"Accept-Encoding": "identity"}),
                        timeout=self.budget.timeout(),
                    ),
                )
            except HTTPError as error:
                if error.code != 503:
                    raise
                with closing(error):
                    http._require_maintenance_response(error, self.budget)
                self.budget.wait_for_maintenance(phase)
                continue
            try:
                self.budget.remaining()
                if response.headers.get("Content-Encoding", "identity") != "identity":
                    raise http.ProbeError("Observer requires unencoded transfer bytes")
            except BaseException:
                response.close()
                raise
            return response

    def search(self) -> tuple[str, str, int, int, str]:
        with closing(self.request(self.search_url, "observer-search")) as response:
            if response.status != 200:
                raise http.ProbeError("Observed search must return HTTP 200")
            document = json.loads(
                http._bounded_read(response, http._JSON_LIMIT, self.budget)
            )
        if not isinstance(document, dict):
            raise http.ProbeError("Observed search must return a JSON object")
        try:
            publication = document["publications"][0]
            links = publication["links"]
            sizes = [
                link.get("size")
                for link in links
                if isinstance(link.get("rel"), str)
                and http._ACQUISITION.search(link["rel"])
            ]
            size = sizes[0]
        except (KeyError, IndexError, TypeError, AttributeError) as error:
            raise http.ProbeError("Observed publication has no acquisition") from error
        if type(size) is not int or not 0 < size < 2**63:
            raise http.ProbeError("Observed acquisition size must be a positive int63")
        link = http._link(document, self.old.gid, size)
        acquisition = http._acquisition_url(self.base, link.get("href"))
        revision = _revision(acquisition)
        navigation = _self_link(document, self.base)
        if _revision(navigation) != revision:
            raise http.ProbeError("Feed and acquisition revisions disagree")
        title = publication.get("metadata", {}).get("title")
        if not isinstance(title, str) or not title:
            raise http.ProbeError("Observed publication has no title")
        return acquisition, navigation, revision, size, title

    def digest(self, response: http._Response, size: int) -> str:
        if response.status != 200 or response.headers.get("Content-Length") != str(
            size
        ):
            raise http.ProbeError("Observed archive has incorrect status or size")
        digest = sha256()
        count = 0
        while count <= size:
            self.budget.remaining()
            value = response.read(min(http._CHUNK_BYTES, size + 1 - count))
            self.budget.remaining()
            if not value:
                break
            count += len(value)
            if count > size:
                raise http.ProbeError("Observed archive exceeded its advertised size")
            digest.update(value)
        if count != size or response.headers.get("ETag") != f'"{digest.hexdigest()}"':
            raise http.ProbeError(
                "Observed archive bytes disagree with its sealed ETag"
            )
        return digest.hexdigest()

    def sample(self) -> dict[str, object]:
        started = perf_counter()
        acquisition, _navigation, revision, size, title = self.search()
        result: dict[str, object] = {
            "revision": revision,
            "byte_length": size,
            "title": title,
            "started_seconds": started - self.budget.started,
        }
        try:
            response = self.request(acquisition, "observer-download")
        except HTTPError as error:
            with closing(error):
                if error.code != 404:
                    raise
                body = json.loads(
                    http._bounded_read(cast(http._Response, error), 4096, self.budget)
                )
                if error.headers.get("Cache-Control") != "no-store" or body != {
                    "detail": f"Catalog revision {revision} not found"
                }:
                    raise http.ProbeError(
                        "Unexpected missing acquisition response"
                    ) from error
            result["status"] = "revision_changed_before_download"
        else:
            with closing(response):
                result.update(status="verified", sha256=self.digest(response, size))
        result["completed_seconds"] = perf_counter() - self.budget.started
        self.samples.append(result)
        if len(self.samples) > 16384:
            raise http.ProbeError("Observer exceeded its bounded sample count")
        return result

    def stale_navigation(self, url: str, final: Artifact) -> dict[str, object]:
        try:
            response = self.request(url, "stale-navigation")
        except HTTPError as error:
            with closing(error):
                if (
                    error.code != 303
                    or error.headers.get("Cache-Control") != "no-store"
                ):
                    raise http.ProbeError(
                        "Stale navigation must return a noncached 303"
                    ) from error
                location = http._acquisition_url(
                    self.base, error.headers.get("Location")
                )
                requested = urlsplit(self.search_url)
                refreshed = urlsplit(location)
                if refreshed.path != requested.path or parse_qs(
                    refreshed.query
                ) != parse_qs(requested.query):
                    raise http.ProbeError(
                        "Stale navigation changed search or retained its old revision"
                    ) from error
        else:
            response.close()
            raise http.ProbeError("Stale navigation was accepted instead of refreshed")
        current = self.sample()
        _require_sample(current, final)
        return {
            "status": 303,
            "refreshed_revision": current["revision"],
            "query_preserved": True,
        }

    def observe(
        self, *, control: Path, evidence: Path, token: str
    ) -> dict[str, object]:
        acquisition, navigation, revision, size, _title = self.search()
        if (revision, size) != (self.old.revision, self.old.byte_length):
            raise http.ProbeError(
                "Observer did not start from the declared old publication"
            )
        with closing(self.request(acquisition, "held-download")) as held:
            if (
                held.status != 200
                or held.headers.get("Content-Length") != str(self.old.byte_length)
                or held.headers.get("ETag") != f'"{self.old.sha256}"'
            ):
                raise http.ProbeError(
                    "Held response differs from the old sealed archive"
                )
            # Do not issue a second acquisition until the server confirms that
            # this first request owns the gate. Headers can arrive before the
            # response iterator has started in Starlette's worker thread.
            stream_ready = evidence / f"stream-ready-{token}.json"
            while not stream_ready.is_file():
                sleep(min(0.05, self.budget.remaining()))
            # Establish one complete old-revision HTTP read before the runner
            # receives permission to change source bytes. The held request stays
            # paused on its original descriptor throughout this independent read.
            _require_sample(self.sample(), self.old)
            _write(
                evidence / f"http-ready-{token}.json",
                {
                    "status": "ready",
                    "revision": revision,
                    "response_headers_received": True,
                    "server_descriptor_requires_separate_gate_evidence": True,
                },
            )
            finish = control / f"finish-{token}.json"
            while True:
                self.sample()
                if finish.is_file():
                    break
                sleep(min(0.25, self.budget.remaining()))
            final = Artifact.read(finish)
            if final.gid != self.old.gid or final.revision <= self.old.revision:
                raise http.ProbeError(
                    "Observer requires the same GID and an advanced publication"
                )
            old_digest = self.digest(held, self.old.byte_length)
            if old_digest != self.old.sha256:
                raise http.ProbeError(
                    "Held download changed across publication and cleanup"
                )
        current = self.sample()
        _require_sample(current, final)
        stale = self.stale_navigation(navigation, final)
        _require_sample_history(self.samples, self.old, final)
        return {
            "status": "passed",
            "gid": self.old.gid,
            "old_revision": self.old.revision,
            "new_revision": final.revision,
            "held_download_sha256": old_digest,
            "held_download_byte_length": self.old.byte_length,
            "new_download_sha256": final.sha256,
            "samples": self.samples,
            "stale_navigation": stale,
            "maintenance_events": self.budget.maintenance_events,
            "total_seconds": perf_counter() - self.budget.started,
            "limitations": [
                "Polling samples request boundaries; it does not prove uninterrupted availability between samples.",
                "Server descriptor lifetime requires matching stream_gate_reached/released evidence around publication and cleanup.",
                "The deliberate stream gate belongs to correctness testing and must not be included in baseline performance timings.",
            ],
        }


def _revision(url: str) -> int:
    values = parse_qs(urlsplit(url).query).get("revision", [])
    if len(values) != 1 or not values[0].isascii() or not values[0].isdigit():
        raise http.ProbeError("Published link requires one exact revision")
    result = int(values[0])
    if not 0 < result < 2**63:
        raise http.ProbeError("Published link revision is outside int63")
    return result


def _self_link(document: Mapping[str, Any], base: Any) -> str:
    links = document.get("links")
    if not isinstance(links, list):
        raise http.ProbeError("Observed feed has no navigation links")
    selected = [
        link for link in links if isinstance(link, dict) and link.get("rel") == "self"
    ]
    if len(selected) != 1:
        raise http.ProbeError("Observed feed requires exactly one self link")
    return http._acquisition_url(base, selected[0].get("href"))


def _require_sample(sample: Mapping[str, object], artifact: Artifact) -> None:
    if (
        sample.get("status") != "verified"
        or sample.get("revision") != artifact.revision
        or sample.get("sha256") != artifact.sha256
        or sample.get("byte_length") != artifact.byte_length
    ):
        raise http.ProbeError(
            "New feed and acquisition disagree with the final independent oracle"
        )


def _require_sample_history(
    samples: list[dict[str, object]], old: Artifact, final: Artifact
) -> None:
    verified = [sample for sample in samples if sample["status"] == "verified"]
    revisions = [cast(int, sample["revision"]) for sample in samples]
    allowed = {(old.sha256, old.byte_length), (final.sha256, final.byte_length)}
    if (
        len(verified) < 2
        or revisions != sorted(revisions)
        or any(not old.revision <= revision <= final.revision for revision in revisions)
        or any(
            (sample["sha256"], sample["byte_length"]) not in allowed
            for sample in verified
        )
    ):
        raise http.ProbeError(
            "Concurrent downloads exposed an unexpected archive or revision"
        )
    for sample in verified:
        match sample["revision"]:
            case old.revision:
                _require_sample(sample, old)
            case final.revision:
                _require_sample(sample, final)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--gid", type=int, required=True)
    parser.add_argument("--revision", type=int, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--control-directory", type=Path, required=True)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    parser.add_argument("--deadline-seconds", type=float, default=300)
    args = parser.parse_args(argv)
    if _TOKEN.fullmatch(args.token) is None:
        parser.error("Observer token must be a bounded safe identifier")
    for directory in (args.control_directory, args.evidence_directory):
        if (
            not directory.is_absolute()
            or directory.is_symlink()
            or not directory.is_dir()
        ):
            parser.error(
                "Observer directories must be existing absolute real directories"
            )
    result_path = args.evidence_directory / f"http-result-{args.token}.json"
    try:
        observer = Observer(
            args.base_url,
            Artifact(args.gid, args.revision, args.sha256, args.size),
            deadline_seconds=args.deadline_seconds,
        )
    except ValueError as error:
        parser.error(str(error))
    stopped = threading.Event()

    def hard_deadline() -> None:
        # A detached observer must not outlive its bounded correctness scenario,
        # even if a peer keeps a blocking read alive with a trickle of bytes.
        if not stopped.wait(args.deadline_seconds):
            try:
                _write(
                    result_path,
                    {"status": "failed", "failure": "observer hard deadline exceeded"},
                )
            finally:
                os._exit(124)

    watchdog = threading.Thread(target=hard_deadline, daemon=True)
    watchdog.start()
    try:
        result = observer.observe(
            control=args.control_directory,
            evidence=args.evidence_directory,
            token=args.token,
        )
        code = 0
    except Exception as error:
        result = {
            "status": "failed",
            "failure": f"{type(error).__name__}: {error}",
            "samples": observer.samples,
            "maintenance_events": observer.budget.maintenance_events,
        }
        code = 1
    finally:
        stopped.set()
        watchdog.join()
    _write(result_path, result)
    print(
        json.dumps(result, sort_keys=True), file=sys.stdout if code == 0 else sys.stderr
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
