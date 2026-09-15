"""Real synthetic gallery bytes and an independent public catalog/CBZ oracle.

Pillow is an explicit integration-environment prerequisite, not a core runtime
dependency. Nothing here imports ingest internals or writes database relations.
The manifest distinguishes current source bytes from the last completed version
so an interrupted producer can be checked without accepting a partial publication.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import shutil
import tempfile
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from hashlib import file_digest, sha256, shake_256
from io import BytesIO
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, cast
from zipfile import ZipFile

from h2hdb import (
    CatalogPublication,
    CatalogReader,
    CatalogRevision,
    CoreConfig,
    DatabaseAccessMode,
    open_database,
    resolve_environment_placeholders,
)

Profile = Literal["small", "large", "mixed"]
MarkerState = Literal["complete", "pending", "missing"]
MANIFEST_NAME = ".acceptance-manifest.json"
_EPOCH_NS = 1_704_067_200_000_000_000
_GRID = 16


@dataclass(frozen=True)
class RenderContract:
    """Resolved public ingest policy; reference rendering remains independent."""

    max_image_short_side: int = 768
    max_image_long_side: int = 8192
    page_jpeg_quality: int = 90
    thumbnail_jpeg_quality: int = 85
    resampler: str = "lanczos"
    optimize: bool = True
    format: str = "JPEG"
    maximum_rgb_rms: float = 16.0


DEFAULT_RENDER_CONTRACT = RenderContract()


def resolve_render_contract(paths: object) -> RenderContract:
    # Public config resolves presets/defaults. Do not call the implementation's
    # resize, image-loading or JPEG functions to build the independent oracle.
    resolved = importlib.import_module("h2hdb_ingest").IngestPathsConfig.model_validate(
        paths
    )
    policy = resolved.render_policy
    return RenderContract(
        max_image_short_side=resolved.max_image_short_side,
        page_jpeg_quality=policy.page_jpeg_quality,
        thumbnail_jpeg_quality=policy.thumbnail_jpeg_quality,
        resampler=policy.resampler.value,
        optimize=policy.optimize,
    )


@dataclass(frozen=True)
class PageFixture:
    name: str
    sha256: str
    byte_length: int
    width: int
    height: int
    identity: str


@dataclass(frozen=True)
class GalleryVersion:
    generation: int
    profile: Profile
    title: str
    marker_text: str
    marker_sha256: str
    mtime_ns: int
    pages: tuple[PageFixture, ...]


@dataclass(frozen=True)
class GalleryFixture:
    gid: int
    marker_state: MarkerState
    current: GalleryVersion
    expected: GalleryVersion | None
    collection: str | None = None


@dataclass(frozen=True)
class FixtureManifest:
    galleries: tuple[GalleryFixture, ...]
    schema: int = 2

    def write(self, path: Path) -> None:
        _write_json(path, asdict(self))


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _version(raw: dict[str, Any]) -> GalleryVersion:
    value = GalleryVersion(
        **{key: value for key, value in raw.items() if key != "pages"},
        pages=tuple(PageFixture(**page) for page in raw["pages"]),
    )
    _require(value.generation >= 1, "manifest generation must be positive")
    _require(value.profile in {"small", "large", "mixed"}, "unknown image profile")
    _require(1 <= len(value.pages) <= 4096, "manifest page count outside 1..4096")
    _require(
        value.marker_sha256 == sha256(value.marker_text.encode()).hexdigest(),
        "manifest marker digest disagrees with its text",
    )
    _require(
        [page.name for page in value.pages]
        == [f"{index + 1:04d}.png" for index in range(len(value.pages))],
        "manifest source pages are not dense canonical names",
    )
    for page in value.pages:
        _require(
            page.width >= 128 and page.height >= 128 and page.byte_length > 0,
            "invalid source page dimensions or byte length",
        )
        _require(
            len(bytes.fromhex(page.sha256)) == len(bytes.fromhex(page.identity)) == 32,
            "invalid source page digest or identity",
        )
    return value


def read_manifest(path: Path) -> FixtureManifest:
    raw = json.loads(path.read_text(encoding="utf-8"))
    _require(raw["schema"] == 2, "unsupported acceptance manifest schema")
    galleries = tuple(
        GalleryFixture(
            gid=item["gid"],
            marker_state=item["marker_state"],
            current=_version(item["current"]),
            expected=None if item["expected"] is None else _version(item["expected"]),
            collection=item["collection"],
        )
        for item in raw["galleries"]
    )
    _require(
        all(type(gallery.gid) is int and gallery.gid > 0 for gallery in galleries),
        "manifest GIDs must be positive integers",
    )
    _require(
        len({gallery.gid for gallery in galleries}) == len(galleries),
        "duplicate GID in acceptance manifest",
    )
    for gallery in galleries:
        _require(
            gallery.collection is None
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", gallery.collection)
            is not None,
            "invalid fixture collection name",
        )
        _require(
            gallery.marker_state in {"complete", "pending", "missing"},
            "invalid completion marker state",
        )
        if gallery.marker_state == "complete":
            _require(gallery.expected == gallery.current, "completed version mismatch")
        for version in (gallery.current, gallery.expected):
            if version is None:
                continue
            for index, page in enumerate(version.pages):
                _require(
                    page.identity
                    == page_identity(gallery.gid, index, version.generation),
                    "page identity does not encode its GID, page and generation",
                )
    return FixtureManifest(tuple(sorted(galleries, key=lambda gallery: gallery.gid)))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _image_module() -> Any:
    try:
        return importlib.import_module("PIL.Image")
    except ImportError as error:
        raise RuntimeError(
            "Pillow is required in the explicit deployment acceptance environment"
        ) from error


def page_identity(gid: int, index: int, generation: int) -> str:
    """Encode all integer bits; unlike RGB modulo fixtures this does not cycle."""
    return sha256(
        f"h2hdb-acceptance-v1:{gid}:{index}:{generation}".encode()
    ).hexdigest()


def image_dimensions(profile: Profile, gid: int, index: int) -> tuple[int, int]:
    if profile == "small":
        return 128, 192
    if profile == "large":
        return 1024, 1536
    if profile == "mixed":
        # An explicitly described mixture, not a claim of a representative corpus.
        return (1024, 1536) if (gid + index) % 20 == 0 else (128, 192)
    raise ValueError(f"Unknown synthetic image profile: {profile}")


def _page_bytes(identity: str, width: int, height: int) -> bytes:
    """A real raster with deterministic entropy and a JPEG-stable identity patch."""
    pixels = bytearray(shake_256(bytes.fromhex(identity)).digest(width * height))
    bits = int(identity, 16)
    cell = width // _GRID
    for bit in range(256):
        value = 224 if (bits >> (255 - bit)) & 1 else 32
        left, top = (bit % _GRID) * cell, (bit // _GRID) * cell
        for row in range(top, top + cell):
            start = row * width + left
            pixels[start : start + cell] = bytes([value]) * cell
    return bytes(pixels)


def _write_page(
    path: Path, gid: int, index: int, generation: int, profile: Profile
) -> PageFixture:
    identity = page_identity(gid, index, generation)
    width, height = image_dimensions(profile, gid, index)
    image_module = _image_module()
    with image_module.frombytes(
        "L", (width, height), _page_bytes(identity, width, height)
    ) as grayscale:
        with grayscale.convert("RGB") as image:
            image.save(path, format="PNG", compress_level=1)
    return PageFixture(
        path.name, _sha256_file(path), path.stat().st_size, width, height, identity
    )


def _marker_text(gid: int, generation: int) -> tuple[str, str]:
    title = f"Acceptance gallery {gid} generation {generation}"
    downloaded = datetime(2024, 2, 3, 4, 5, tzinfo=UTC) + timedelta(minutes=generation)
    return title, (
        f"Title: {title}\n"
        "Upload Time: 2024-01-02 03:04\n"
        "Uploaded By: acceptance-fixture\n"
        f"Downloaded: {downloaded:%Y-%m-%d %H:%M}\n"
        f"Tags: artist:acceptance-{gid}, group:acceptance, language:english\n"
        "Uploader's Comments\n"
        f"Deterministic offline fixture GID={gid}; generation={generation}\n"
        "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n"
    )


def _write_gallery(
    root: Path,
    *,
    gid: int,
    generation: int,
    pages: int,
    profile: Profile,
    marker: MarkerState,
    previous: GalleryFixture | None,
) -> GalleryFixture:
    _require(gid > 0 and generation > 0, "GID and generation must be positive")
    _require(1 <= pages <= 4096, "pages must be in 1..4096")
    _require(marker in {"complete", "pending", "missing"}, "invalid marker state")
    _require(
        previous is None or generation >= previous.current.generation,
        "generation must not move backwards",
    )
    folder = root / str(gid)
    if previous is None:
        folder.mkdir(parents=True, exist_ok=False)
    else:
        _require(folder.is_dir(), f"Expected existing fixture folder: {folder}")
    # One second separates generations; the marker follows every source image.
    image_mtime_ns = _EPOCH_NS + generation * 1_000_000_000
    page_fixtures = []
    for index in range(pages):
        path = folder / f"{index + 1:04d}.png"
        page_fixtures.append(_write_page(path, gid, index, generation, profile))
        os.utime(path, ns=(image_mtime_ns, image_mtime_ns))
    if previous is not None:
        retained = {page.name for page in page_fixtures}
        for old_page in previous.current.pages:
            if old_page.name not in retained:
                (folder / old_page.name).unlink()
    title, marker_text = _marker_text(gid, generation)
    version = GalleryVersion(
        generation,
        profile,
        title,
        marker_text,
        sha256(marker_text.encode()).hexdigest(),
        image_mtime_ns + 1,
        tuple(page_fixtures),
    )
    marker_path = folder / "galleryinfo.txt"
    if marker == "complete":
        temporary = folder / ".galleryinfo.tmp"
        temporary.write_text(marker_text, encoding="utf-8")
        os.utime(temporary, ns=(version.mtime_ns, version.mtime_ns))
        temporary.replace(marker_path)
        expected: GalleryVersion | None = version
    else:
        expected = None if previous is None else previous.expected
        if marker == "missing":
            marker_path.unlink(missing_ok=True)
    return GalleryFixture(gid, marker, version, expected)


def generate(
    root: Path,
    *,
    count: int,
    start_gid: int = 1_000_001,
    pages: int = 2,
    generation: int = 1,
    profile: Profile = "small",
    marker: MarkerState = "complete",
    manifest_path: Path | None = None,
) -> FixtureManifest:
    """Add actual galleries, refusing overlap or untracked existing directories."""
    _require(count > 0, "count must be positive")
    _require(start_gid > 0 and generation > 0, "GID and generation must be positive")
    _require(1 <= pages <= 4096, "pages must be in 1..4096")
    manifest_path = manifest_path or root / MANIFEST_NAME
    existing = (
        read_manifest(manifest_path) if manifest_path.exists() else FixtureManifest(())
    )
    previous_gids = {gallery.gid for gallery in existing.galleries}
    requested = range(start_gid, start_gid + count)
    _require(
        not previous_gids.intersection(requested),
        "generate would replace an existing GID",
    )
    _require(
        all(not (root / str(gid)).exists() for gid in requested),
        "generate would replace an existing folder",
    )
    galleries = list(existing.galleries)
    for gid in requested:
        galleries.append(
            _write_gallery(
                root,
                gid=gid,
                generation=generation,
                pages=pages,
                profile=profile,
                marker=marker,
                previous=None,
            )
        )
    result = FixtureManifest(tuple(sorted(galleries, key=lambda gallery: gallery.gid)))
    result.write(manifest_path)
    return result


def change(
    root: Path,
    *,
    gid: int,
    generation: int,
    marker: MarkerState = "complete",
    pages: int | None = None,
    manifest_path: Path | None = None,
) -> FixtureManifest:
    manifest_path = manifest_path or root / MANIFEST_NAME
    manifest = read_manifest(manifest_path)
    old = next((gallery for gallery in manifest.galleries if gallery.gid == gid), None)
    _require(old is not None, f"GID {gid} is not owned by this fixture manifest")
    assert old is not None
    replacement = _write_gallery(
        root if old.collection is None else root / old.collection,
        gid=gid,
        generation=generation,
        pages=len(old.current.pages) if pages is None else pages,
        profile=old.current.profile,
        marker=marker,
        previous=old,
    )
    replacement = replace(replacement, collection=old.collection)
    result = FixtureManifest(
        tuple(
            replacement if gallery.gid == gid else gallery
            for gallery in manifest.galleries
        )
    )
    result.write(manifest_path)
    return result


def append_collection(
    root: Path,
    *,
    count: int,
    start_gid: int,
    pages: int,
    profile: Profile,
    collection: str,
) -> FixtureManifest:
    """Expose a fully generated group with one same-filesystem directory rename."""
    _require(
        re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", collection) is not None,
        "invalid fixture collection name",
    )
    manifest_path = root / MANIFEST_NAME
    existing = read_manifest(manifest_path)
    requested = set(range(start_gid, start_gid + count))
    _require(
        not requested.intersection(gallery.gid for gallery in existing.galleries),
        "collection append would replace an existing GID",
    )
    target = root / collection
    _require(not target.exists() and not target.is_symlink(), "collection exists")
    staging = Path(tempfile.mkdtemp(prefix=".acceptance-collection-", dir=root.parent))
    try:
        added = generate(
            staging,
            count=count,
            start_gid=start_gid,
            pages=pages,
            profile=profile,
        )
        (staging / MANIFEST_NAME).unlink()
        result = FixtureManifest(
            tuple(
                sorted(
                    (
                        *existing.galleries,
                        *(
                            replace(row, collection=collection)
                            for row in added.galleries
                        ),
                    ),
                    key=lambda row: row.gid,
                )
            )
        )
        # The oracle must already expect the new group when ingest can see it.
        result.write(manifest_path)
        try:
            staging.rename(target)
        except OSError:
            existing.write(manifest_path)
            raise
        return result
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def rendered_dimensions(
    page: PageFixture, contract: RenderContract = DEFAULT_RENDER_CONTRACT
) -> tuple[int, int]:
    """No enlargement; preserve aspect within integer short/long-side bounds."""
    scale = min(
        Fraction(1),
        Fraction(contract.max_image_short_side, min(page.width, page.height)),
        Fraction(contract.max_image_long_side, max(page.width, page.height)),
    )
    return max(1, int(page.width * scale)), max(1, int(page.height * scale))


def _check_image(
    encoded: bytes,
    expected: PageFixture,
    contract: RenderContract = DEFAULT_RENDER_CONTRACT,
) -> float:
    dimensions = rendered_dimensions(expected, contract)
    _require(
        70 <= contract.page_jpeg_quality <= 95 and min(dimensions) >= 128,
        "independent fixture pixel oracle requires JPEG quality 70..95 and output short side >=128",
    )
    with _image_module().open(BytesIO(encoded)) as image:
        image.load()
        _require(image.format == "JPEG", "archive page is not JPEG")
        _require(
            image.size == dimensions,
            f"archive image dimensions {image.size} differ from independently expected {dimensions}",
        )
        with image.convert("L") as grayscale:
            cell = expected.width // _GRID
            bits = 0
            for bit in range(256):
                x, y = (
                    ((bit % _GRID) * cell + cell // 2) * image.width // expected.width,
                    ((bit // _GRID) * cell + cell // 2)
                    * image.height
                    // expected.height,
                )
                bits = (bits << 1) | int(grayscale.getpixel((x, y)) >= 128)
            _require(
                f"{bits:064x}" == expected.identity,
                "archive image encodes a different GID, page or generation",
            )
            # The identity patch proves ordering/version, while this independent
            # complete-raster comparison detects altered pixels elsewhere. The
            # fixture is grayscale. Resize the independent original raster using
            # the configured public filter; do not use the production libvips or
            # ingest transform. RMS <= 16 accommodates the tested real ingest
            # Q90/Lanczos and Q70/Bilinear conversion paths, including reduction.
            with (
                _image_module().frombytes(
                    "L",
                    (expected.width, expected.height),
                    _page_bytes(expected.identity, expected.width, expected.height),
                ) as reference,
                reference.convert("RGB") as reference_rgb,
                image.convert("RGB") as actual_rgb,
            ):
                reference_rgb.thumbnail(
                    dimensions,
                    getattr(_image_module().Resampling, contract.resampler.upper()),
                )
                difference = importlib.import_module("PIL.ImageChops").difference(
                    actual_rgb, reference_rgb
                )
                with difference:
                    rms = importlib.import_module("PIL.ImageStat").Stat(difference).rms
                    _require(
                        max(rms) <= contract.maximum_rgb_rms,
                        "archive image pixels differ from the independent source raster",
                    )
                    return float(max(rms))


def _check_sources(source: Path, manifest: FixtureManifest) -> int:
    byte_count = 0
    seen: set[str] = set()
    _require(
        {path.name for path in source.iterdir() if path.is_dir()}
        == {gallery.collection or str(gallery.gid) for gallery in manifest.galleries},
        "source gallery directories differ from the fixture manifest",
    )
    for collection in {row.collection for row in manifest.galleries} - {None}:
        assert collection is not None
        folder = source / collection
        _require(not folder.is_symlink(), "fixture collection must not be a symlink")
        _require(
            {path.name for path in folder.iterdir()}
            == {
                str(row.gid)
                for row in manifest.galleries
                if row.collection == collection
            },
            "collection gallery directories differ from the fixture manifest",
        )
    for gallery in manifest.galleries:
        parent = source if gallery.collection is None else source / gallery.collection
        folder = parent / str(gallery.gid)
        actual_names = {
            path.name for path in folder.iterdir() if path.name != "galleryinfo.txt"
        }
        _require(
            actual_names == {page.name for page in gallery.current.pages},
            f"GID {gallery.gid}: source page set changed",
        )
        for page in gallery.current.pages:
            path = folder / page.name
            _require(
                _sha256_file(path) == page.sha256,
                f"GID {gallery.gid}: source page digest mismatch",
            )
            _require(
                path.stat().st_size == page.byte_length,
                "source page byte length mismatch",
            )
            _require(page.sha256 not in seen, "synthetic source images are not unique")
            seen.add(page.sha256)
            byte_count += page.byte_length
        marker = folder / "galleryinfo.txt"
        if gallery.marker_state == "missing" or gallery.expected is None:
            _require(
                not marker.exists(), f"GID {gallery.gid}: marker unexpectedly exists"
            )
        else:
            _require(
                _sha256_file(marker) == gallery.expected.marker_sha256,
                f"GID {gallery.gid}: completion marker digest mismatch",
            )
            newest_image = max(
                (folder / page.name).stat().st_mtime_ns
                for page in gallery.current.pages
            )
            if gallery.marker_state == "complete":
                _require(
                    newest_image <= marker.stat().st_mtime_ns,
                    "completed fixture has an image newer than its marker",
                )
            else:
                _require(
                    newest_image > marker.stat().st_mtime_ns,
                    "pending fixture does not have a newer image than its marker",
                )
    return byte_count


def _storage_path(current: Path, segments: tuple[str, ...]) -> Path:
    path = current.joinpath(*segments)
    _require(
        path.resolve().is_relative_to(current.resolve()),
        "artifact escapes the fixture library",
    )
    return path


def _verify_publication(
    catalog: CatalogReader,
    revision: CatalogRevision,
    publication: CatalogPublication,
    expected: GalleryVersion,
    current: Path,
    contract: RenderContract,
) -> dict[str, object]:
    _require(
        publication.source_title == expected.title,
        f"GID {publication.gid}: source title differs",
    )
    _require(
        publication.source_gallery_name == str(publication.gid),
        "publication source folder differs",
    )
    _require(
        publication.page_count == len(expected.pages), "publication page count differs"
    )
    _require(len(publication.artifacts) == 1, "publication must expose one CBZ")
    storage = publication.artifacts[0].storage_object
    archive_path = _storage_path(current, storage.key.segments)
    stat = archive_path.stat()
    _require(
        stat.st_size == storage.size_bytes,
        "CBZ size differs from its public descriptor",
    )
    _require(
        _sha256_file(archive_path) == storage.sha256,
        "CBZ differs from its public descriptor",
    )
    page_digests = []
    page_dimensions = []
    page_rms = []
    with ZipFile(archive_path) as archive, archive_path.open("rb") as extent_reader:
        names = [
            "galleryinfo.txt",
            *(f"pages/{index:04d}.jpg" for index in range(len(expected.pages))),
        ]
        _require(
            archive.namelist() == names,
            "CBZ members are not the expected dense page sequence",
        )
        # ZipFile.read checks each member CRC; reading every member once is also
        # a complete CRC check, without an unnecessary second testzip pass.
        _require(
            archive.read("galleryinfo.txt") == expected.marker_text.encode(),
            "CBZ marker does not match the completed fixture version",
        )
        for index, page in enumerate(expected.pages):
            encoded = archive.read(f"pages/{index:04d}.jpg")
            page_rms.append(_check_image(encoded, page, contract))
            dimensions = rendered_dimensions(page, contract)
            page_dimensions.append(list(dimensions))
            resource = catalog.get_publication_page(
                publication.publication_id, index, revision=revision
            )
            _require(
                resource is not None and resource.storage_object == storage,
                "public page descriptor does not reference the CBZ",
            )
            assert resource is not None
            _require(
                (resource.width, resource.height) == dimensions,
                "public page dimensions differ",
            )
            _require(
                sha256(encoded).hexdigest() == resource.sha256,
                "public page digest differs from CBZ bytes",
            )
            extent_reader.seek(resource.extent.offset)
            _require(
                extent_reader.read(resource.extent.length) == encoded,
                "public page extent differs from ZIP member",
            )
            if index == 0:
                _require(
                    publication.cover == resource,
                    "cover does not reference the first page",
                )
            page_digests.append(resource.sha256)
    _require(publication.thumbnail is not None, "thumbnail missing")
    assert publication.thumbnail is not None
    thumbnail = publication.thumbnail.storage_object
    thumbnail_path = _storage_path(current, thumbnail.key.segments)
    _require(
        thumbnail_path.stat().st_size == thumbnail.size_bytes, "thumbnail size differs"
    )
    _require(
        _sha256_file(thumbnail_path) == thumbnail.sha256, "thumbnail digest differs"
    )
    with _image_module().open(thumbnail_path) as image:
        image.load()
        _require(
            image.format == "JPEG" and max(image.size) <= 320,
            "thumbnail is not a bounded JPEG",
        )
    after = archive_path.stat()
    _require(
        (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        == (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns),
        "CBZ changed during verification",
    )
    return {
        "gid": publication.gid,
        "generation": expected.generation,
        "pages": len(expected.pages),
        "key": "/".join(storage.key.segments),
        "sha256": storage.sha256,
        "byte_length": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "ctime_ns": stat.st_ctime_ns,
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "thumbnail_key": "/".join(thumbnail.key.segments),
        "thumbnail_sha256": thumbnail.sha256,
        "page_sha256": page_digests,
        "page_dimensions": page_dimensions,
        "page_rgb_rms": page_rms,
    }


def verify_catalog(
    catalog: CatalogReader,
    *,
    source: Path,
    library: Path,
    manifest: FixtureManifest,
    render_contract: RenderContract = DEFAULT_RENDER_CONTRACT,
) -> dict[str, object]:
    """Verify all pages and publications using an independently created manifest."""
    started = perf_counter()
    source_bytes = _check_sources(source, manifest)
    expected = {
        gallery.gid: gallery.expected
        for gallery in manifest.galleries
        if gallery.expected is not None
    }
    revision = catalog.get_catalog_revision()
    _require(
        revision.publication_count == len(expected),
        "catalog revision publication count differs",
    )
    seen: set[int] = set()
    artifacts = []
    cursor = None
    discovery_pages = 0
    while True:
        page = catalog.discover_publications(after=cursor, limit=128, revision=revision)
        discovery_pages += 1
        _require(page.total in {None, len(expected)}, "discovery total differs")
        for publication in page.publications:
            _require(
                publication.gid not in seen, "duplicate GID across discovery pages"
            )
            _require(
                publication.gid in expected,
                f"unexpected published GID {publication.gid}",
            )
            version = expected[publication.gid]
            assert version is not None
            artifacts.append(
                _verify_publication(
                    catalog,
                    revision,
                    publication,
                    version,
                    library / "current",
                    render_contract,
                )
            )
            seen.add(publication.gid)
        if page.next_cursor is None:
            break
        _require(
            page.publications != () and page.next_cursor != cursor,
            "discovery cursor made no progress",
        )
        cursor = page.next_cursor
    _require(seen == set(expected), "catalog omitted expected GIDs")
    _require(
        catalog.get_catalog_revision() == revision,
        "publication head changed during verification",
    )
    _require(
        not (library / ".h2hdb-coordination" / "ACTIVATING").exists(),
        "library activation is unfinished",
    )
    return {
        "schema": 1,
        "render_contract": asdict(render_contract),
        "revision": revision.revision,
        "actual_source_galleries": len(manifest.galleries),
        "actual_source_pages": sum(
            len(gallery.current.pages) for gallery in manifest.galleries
        ),
        "actual_source_bytes": source_bytes,
        "verified_publications": len(seen),
        "verified_pages": sum(
            len(version.pages) for version in expected.values() if version is not None
        ),
        "discovery_pages": discovery_pages,
        "verification_seconds": perf_counter() - started,
        "artifacts": sorted(artifacts, key=lambda item: cast(int, item["gid"])),
    }


def verify(
    *,
    config: Path,
    source: Path,
    library: Path,
    expected_manifest: Path,
    output: Path,
    render_config: Path | None = None,
) -> dict[str, object]:
    raw = json.loads(config.read_text(encoding="utf-8"))
    render_raw = (
        raw
        if render_config is None
        else json.loads(render_config.read_text(encoding="utf-8"))
    )
    contract = resolve_render_contract(
        resolve_environment_placeholders(
            render_raw.get(
                "paths", {"download_path": str(source), "library_path": str(library)}
            )
        )
    )
    core = CoreConfig.model_validate(
        resolve_environment_placeholders(raw.get("core", raw))
    )
    core = core.model_copy(
        update={
            "database": core.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )
    started = perf_counter()
    with closing(open_database(core)) as catalog:
        opened = perf_counter()
        report = verify_catalog(
            catalog,
            source=source,
            library=library,
            manifest=read_manifest(expected_manifest),
            render_contract=contract,
        )
    report["open_database_ready_audit_seconds"] = opened - started
    report["total_verification_seconds"] = perf_counter() - started
    _write_json(output, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generator = commands.add_parser("generate")
    generator.add_argument("--root", type=Path, required=True)
    generator.add_argument("--count", type=int, required=True)
    generator.add_argument("--start-gid", type=int, default=1_000_001)
    generator.add_argument("--pages", type=int, default=2)
    generator.add_argument("--generation", type=int, default=1)
    generator.add_argument(
        "--profile", choices=("small", "large", "mixed"), default="small"
    )
    generator.add_argument(
        "--marker", choices=("complete", "pending", "missing"), default="complete"
    )
    generator.add_argument("--manifest", type=Path)
    appender = commands.add_parser("append-collection")
    appender.add_argument("--root", type=Path, required=True)
    appender.add_argument("--count", type=int, required=True)
    appender.add_argument("--start-gid", type=int, required=True)
    appender.add_argument("--pages", type=int, required=True)
    appender.add_argument(
        "--profile", choices=("small", "large", "mixed"), required=True
    )
    appender.add_argument("--collection", required=True)
    changer = commands.add_parser("change")
    changer.add_argument("--root", type=Path, required=True)
    changer.add_argument("--gid", type=int, required=True)
    changer.add_argument("--generation", type=int, required=True)
    changer.add_argument("--pages", type=int)
    changer.add_argument(
        "--marker", choices=("complete", "pending", "missing"), default="complete"
    )
    changer.add_argument("--manifest", type=Path)
    verifier = commands.add_parser("verify")
    verifier.add_argument("--render-config", type=Path)
    for name in ("config", "source", "library", "expected-manifest", "output"):
        verifier.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "generate":
        result = generate(
            args.root,
            count=args.count,
            start_gid=args.start_gid,
            pages=args.pages,
            generation=args.generation,
            profile=args.profile,
            marker=args.marker,
            manifest_path=args.manifest,
        )
        print(
            json.dumps(
                {
                    "actual_source_galleries": len(result.galleries),
                    "actual_source_pages": sum(
                        len(gallery.current.pages) for gallery in result.galleries
                    ),
                    "manifest": str(args.manifest or args.root / MANIFEST_NAME),
                }
            )
        )
    elif args.command == "append-collection":
        result = append_collection(
            args.root,
            count=args.count,
            start_gid=args.start_gid,
            pages=args.pages,
            profile=args.profile,
            collection=args.collection,
        )
        print(json.dumps({"actual_source_galleries": len(result.galleries)}))
    elif args.command == "change":
        change(
            args.root,
            gid=args.gid,
            generation=args.generation,
            marker=args.marker,
            pages=args.pages,
            manifest_path=args.manifest,
        )
    else:
        report = verify(
            config=args.config,
            source=args.source,
            library=args.library,
            expected_manifest=args.expected_manifest,
            output=args.output,
            render_config=args.render_config,
        )
        print(
            json.dumps(
                {key: value for key, value in report.items() if key != "artifacts"},
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
