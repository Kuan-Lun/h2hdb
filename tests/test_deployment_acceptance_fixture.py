"""Unit contracts plus an opt-in, real installed-consumer SQLite exercise.

Set H2HDB_ACCEPTANCE_PYTHON to an explicit interpreter containing the ingest
integration packages. No sibling checkout discovery, private data or service is
used. Tiny synthetic runs are reported as tiny runs, never as a scale benchmark.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "deployment_acceptance"
    / "fixture.py"
)


def _module() -> ModuleType:
    name = "deployment_acceptance_fixture_under_test"
    specification = importlib.util.spec_from_file_location(name, _SCRIPT)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


@pytest.fixture
def fixture_module() -> ModuleType:
    return _module()


def _manifest(module: ModuleType, count: int = 1) -> Any:
    galleries = []
    for gid in range(1_000_001, 1_000_001 + count):
        title, marker = module._marker_text(gid, 1)
        page = module.PageFixture(
            "0001.png",
            sha256(str(gid).encode()).hexdigest(),
            100,
            128,
            192,
            module.page_identity(gid, 0, 1),
        )
        version = module.GalleryVersion(
            1,
            "small",
            title,
            marker,
            sha256(marker.encode()).hexdigest(),
            1_704_067_201_000_000_001,
            (page,),
        )
        galleries.append(module.GalleryFixture(gid, "complete", version, version))
    return module.FixtureManifest(tuple(galleries))


def test_identity_uses_full_gid_page_and_generation(fixture_module: ModuleType) -> None:
    identities = {
        fixture_module.page_identity(gid, index, generation)
        for gid in (1, 257, 65_537, 1_000_001, 2**62)
        for index in (0, 1, 4095)
        for generation in (1, 2, 257)
    }
    assert len(identities) == 45
    assert fixture_module.page_identity(
        1_000_001, 0, 1
    ) == fixture_module.page_identity(1_000_001, 0, 1)
    assert fixture_module.image_dimensions("small", 1, 0) == (128, 192)
    assert fixture_module.image_dimensions("large", 1, 0) == (1024, 1536)
    assert fixture_module.image_dimensions("mixed", 20, 0) == (1024, 1536)
    assert fixture_module.image_dimensions("mixed", 21, 0) == (128, 192)


@pytest.mark.parametrize(
    "width,height,short_side,expected",
    [
        (128, 192, 768, (128, 192)),
        (1024, 1536, 768, (768, 1152)),
        (1024, 1536, 256, (256, 384)),
        (512, 16384, 768, (256, 8192)),
    ],
)
def test_render_dimensions_apply_public_no_enlargement_and_two_axis_bounds(
    fixture_module: ModuleType,
    width: int,
    height: int,
    short_side: int,
    expected: tuple[int, int],
) -> None:
    page = replace(
        _manifest(fixture_module).galleries[0].current.pages[0],
        width=width,
        height=height,
    )
    contract = fixture_module.RenderContract(max_image_short_side=short_side)
    assert fixture_module.rendered_dimensions(page, contract) == expected


def test_manifest_retains_completed_version_while_source_changes(
    fixture_module: ModuleType, tmp_path: Path
) -> None:
    manifest = _manifest(fixture_module)
    old = manifest.galleries[0]
    title, marker = fixture_module._marker_text(old.gid, 2)
    page = replace(
        old.current.pages[0], identity=fixture_module.page_identity(old.gid, 0, 2)
    )
    changed = replace(
        old.current,
        generation=2,
        title=title,
        marker_text=marker,
        marker_sha256=sha256(marker.encode()).hexdigest(),
        pages=(page,),
    )
    pending = replace(old, marker_state="pending", current=changed)
    path = tmp_path / "manifest.json"
    fixture_module.FixtureManifest((pending,)).write(path)
    loaded = fixture_module.read_manifest(path)
    assert loaded.galleries[0].current.generation == 2
    assert loaded.galleries[0].expected.generation == 1
    assert loaded == fixture_module.FixtureManifest((pending,))


@pytest.mark.parametrize(
    "corruption",
    ["duplicate", "marker_digest", "page_identity", "page_name", "completed_version"],
)
def test_manifest_rejects_inconsistent_oracle(
    fixture_module: ModuleType, tmp_path: Path, corruption: str
) -> None:
    raw = asdict(_manifest(fixture_module))
    raw = json.loads(json.dumps(raw))
    gallery = raw["galleries"][0]
    if corruption == "duplicate":
        raw["galleries"].append(gallery)
    elif corruption == "marker_digest":
        gallery["current"]["marker_sha256"] = "0" * 64
    elif corruption == "page_identity":
        gallery["current"]["pages"][0]["identity"] = "0" * 64
        gallery["expected"] = gallery["current"]
    elif corruption == "page_name":
        gallery["current"]["pages"][0]["name"] = "../0001.png"
    else:
        gallery["expected"] = None
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError):
        fixture_module.read_manifest(path)


def test_catalog_oracle_traverses_every_bounded_page(
    fixture_module: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # This is only a traversal unit test. Actual byte/DB checks run separately.
    manifest = _manifest(fixture_module, count=129)
    revision = SimpleNamespace(revision=7, publication_count=129)
    publications = tuple(
        SimpleNamespace(gid=gallery.gid) for gallery in manifest.galleries
    )
    calls = []

    def discover(**kwargs: Any) -> SimpleNamespace:
        calls.append(kwargs)
        assert kwargs["limit"] == 128 and kwargs["revision"] is revision
        if kwargs["after"] is None:
            return SimpleNamespace(
                total=129, publications=publications[:128], next_cursor="last-128"
            )
        assert kwargs["after"] == "last-128"
        return SimpleNamespace(
            total=None, publications=publications[128:], next_cursor=None
        )

    catalog = SimpleNamespace(
        get_catalog_revision=lambda: revision, discover_publications=discover
    )
    monkeypatch.setattr(fixture_module, "_check_sources", lambda *_: 12900)
    monkeypatch.setattr(
        fixture_module,
        "_verify_publication",
        lambda _catalog, _revision, publication, _expected, _current, _contract: {
            "gid": publication.gid
        },
    )
    result = fixture_module.verify_catalog(
        catalog, source=tmp_path, library=tmp_path, manifest=manifest
    )
    assert result["verified_publications"] == 129
    assert result["discovery_pages"] == len(calls) == 2
    assert len(result["artifacts"]) == 129


def test_source_oracle_rejects_unmanifested_work(
    fixture_module: ModuleType, tmp_path: Path
) -> None:
    manifest = _manifest(fixture_module)
    (tmp_path / "1000001").mkdir()
    extra = tmp_path / "1000002"
    extra.mkdir()
    with pytest.raises(ValueError, match="directories differ"):
        fixture_module._check_sources(tmp_path, manifest)
    extra.rmdir()
    (tmp_path / "1000001/0001.png").touch()
    (tmp_path / "1000001/untracked.jpg").touch()
    with pytest.raises(ValueError, match="source page set changed"):
        fixture_module._check_sources(tmp_path, manifest)


@pytest.fixture
def collection_fixture(
    fixture_module: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[ModuleType, Path]:
    def page(path: Path, gid: int, index: int, generation: int, _profile: str) -> Any:
        identity = fixture_module.page_identity(gid, index, generation)
        data = identity.encode()
        path.write_bytes(data)
        return fixture_module.PageFixture(
            path.name, sha256(data).hexdigest(), len(data), 128, 192, identity
        )

    monkeypatch.setattr(fixture_module, "_write_page", page)
    source = tmp_path / "source"
    fixture_module.generate(source, count=1, pages=2)
    return fixture_module, source


def test_collection_append_exposes_all_complete_galleries_in_one_rename(
    collection_fixture: tuple[ModuleType, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    module, source = collection_fixture
    rename = Path.rename
    exposed = []

    def observe(path: Path, target: Path) -> Path:
        assert target == source / "growth-append-1"
        assert not target.exists()
        assert path.parent == source.parent
        assert {entry.name for entry in path.iterdir()} == {"1000002", "1000003"}
        assert all(
            (path / str(gid) / "galleryinfo.txt").is_file()
            for gid in (1_000_002, 1_000_003)
        )
        assert len(module.read_manifest(source / module.MANIFEST_NAME).galleries) == 3
        exposed.append(target)
        return rename(path, target)

    monkeypatch.setattr(Path, "rename", observe)
    manifest = module.append_collection(
        source,
        count=2,
        start_gid=1_000_002,
        pages=2,
        profile="small",
        collection="growth-append-1",
    )
    assert len(exposed) == 1
    assert [row.collection for row in manifest.galleries] == [
        None,
        "growth-append-1",
        "growth-append-1",
    ]
    assert module._check_sources(source, manifest) > 0
    assert not list(source.parent.glob(".acceptance-collection-*"))
    changed = module.change(source, gid=1_000_002, generation=2)
    assert changed.galleries[1].collection == "growth-append-1"
    assert module._check_sources(source, changed) > 0


def test_collection_failed_rename_restores_manifest_and_removes_only_owned_staging(
    collection_fixture: tuple[ModuleType, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    module, source = collection_fixture
    previous = (source / module.MANIFEST_NAME).read_bytes()

    def fail(_path: Path, _target: Path) -> Path:
        raise OSError("injected rename failure")

    monkeypatch.setattr(Path, "rename", fail)
    with pytest.raises(OSError, match="injected"):
        module.append_collection(
            source,
            count=2,
            start_gid=1_000_002,
            pages=2,
            profile="small",
            collection="growth",
        )
    assert (source / module.MANIFEST_NAME).read_bytes() == previous
    assert not (source / "growth").exists()
    assert not list(source.parent.glob(".acceptance-collection-*"))


@pytest.mark.parametrize("collection", ["../escape", "/absolute", "", "growth/nested"])
def test_collection_append_rejects_unsafe_names_before_writing(
    collection_fixture: tuple[ModuleType, Path], collection: str
) -> None:
    module, source = collection_fixture
    with pytest.raises(ValueError, match="collection name"):
        module.append_collection(
            source,
            count=2,
            start_gid=1_000_002,
            pages=2,
            profile="small",
            collection=collection,
        )
    assert not list(source.parent.glob(".acceptance-collection-*"))


def test_collection_append_and_oracle_reject_extra_or_overlapping_galleries(
    collection_fixture: tuple[ModuleType, Path],
) -> None:
    module, source = collection_fixture
    with pytest.raises(ValueError, match="existing GID"):
        module.append_collection(
            source,
            count=2,
            start_gid=1_000_001,
            pages=2,
            profile="small",
            collection="growth",
        )
    manifest = module.append_collection(
        source,
        count=2,
        start_gid=1_000_002,
        pages=2,
        profile="small",
        collection="growth",
    )
    (source / "growth/untracked").mkdir()
    with pytest.raises(ValueError, match="collection gallery directories"):
        module._check_sources(source, manifest)


def _integration_python() -> str:
    configured = os.environ.get("H2HDB_ACCEPTANCE_PYTHON")
    if configured:
        assert Path(configured).is_file(), (
            "H2HDB_ACCEPTANCE_PYTHON must be an explicit interpreter file"
        )
        return configured
    if (
        importlib.util.find_spec("PIL") is not None
        and importlib.util.find_spec("h2hdb_ingest") is not None
    ):
        return sys.executable
    pytest.skip(
        "Pillow/ingest are explicit integration prerequisites; set H2HDB_ACCEPTANCE_PYTHON"
    )


def test_real_fixture_gallery_cbz_and_restart_oracle(tmp_path: Path) -> None:
    interpreter = _integration_python()
    program = r"""
import importlib.util
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import build_runtime
from h2hdb_ingest.scratch import DiskScratch

spec = importlib.util.spec_from_file_location("acceptance_fixture", sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
root = Path(sys.argv[2])
source, library = root / "source", root / "library"
for child in ("current/acquisitions", "current/artwork", ".h2hdb-coordination"):
    (library / child).mkdir(parents=True)
manifest = module.generate(source, count=2, pages=2)
first_bytes = (source / "1000001" / "0001.png").read_bytes()
replica = module.generate(root / "replica", count=2, pages=2)
assert manifest == replica
assert first_bytes == (root / "replica/1000001/0001.png").read_bytes()
assert len({p.sha256 for g in manifest.galleries for p in g.current.pages}) == 4
equal_mtime = (source / "1000002/0001.png").stat().st_mtime_ns
os.utime(source / "1000002/galleryinfo.txt", ns=(equal_mtime, equal_mtime))
try:
    module.generate(source, count=1)
except ValueError:
    pass
else:
    raise AssertionError("fixture overwrote an existing gallery")
config = IngestConfig(core=CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(root / "catalog.sqlite3"))), paths=IngestPathsConfig(download_path=source, library_path=library, page_render_workers=1), resident=ResidentConfig(publication_batch_galleries=10, lease_seconds=30, heartbeat_seconds=5))
config_path = root / "ingest.json"
config_path.write_text(config.model_dump_json(), encoding="utf-8")

def inspect(runtime):
    return module.verify_catalog(runtime.catalog, source=source, library=library, manifest=module.read_manifest(source / module.MANIFEST_NAME))

def run(initialize=False):
    with DiskScratch(library) as scratch, build_runtime(config, temporary_cleanup=scratch.cleanup_page) as runtime:
        if initialize:
            runtime.database_admin.initialize()
        runtime.resident.initialize()
        expected = {g.gid: (g.expected.title, len(g.expected.pages)) for g in module.read_manifest(source / module.MANIFEST_NAME).galleries if g.expected is not None}
        for attempt in range(32):
            assert runtime.resident.process_available(periodic_scan=True)
            actual = {p.gid: (p.source_title, p.page_count) for p in runtime.catalog.discover_publications().publications}
            if actual == expected:
                return inspect(runtime)
        raise AssertionError(f"bounded source drain did not reach expected versions: {actual!r}, {expected!r}")

first = run(initialize=True)
assert first["verified_publications"] == 2 and first["verified_pages"] == 4
restart = run()
assert first["artifacts"] == restart["artifacts"], "unchanged restart rewrote a CBZ"
module.generate(source, start_gid=1000003, count=1, pages=1)
added = run()
assert added["verified_publications"] == 3
assert added["artifacts"][:2] == first["artifacts"]
module.change(source, gid=1000001, generation=2, pages=3, marker="pending")
assert (source / "1000001/0001.png").stat().st_mtime_ns > (source / "1000001/galleryinfo.txt").stat().st_mtime_ns
pending = run()
assert pending["artifacts"] == added["artifacts"], "pending producer replaced a completed gallery"
module.change(source, gid=1000001, generation=2, marker="complete")
completed = run()
assert completed["verified_pages"] == 6
assert completed["artifacts"][0]["sha256"] != first["artifacts"][0]["sha256"]
module.change(source, gid=1000002, generation=2, pages=1, marker="missing")
assert not (source / "1000002/0002.png").exists()
missing = run()
assert missing["artifacts"] == completed["artifacts"], "missing marker removed existing publication"
module.change(source, gid=1000002, generation=2, marker="complete")
restored = run()
module.generate(source, start_gid=1000004, count=1, pages=1, marker="missing")
missing_new = run()
assert missing_new["verified_publications"] == 3
module.change(source, gid=1000004, generation=1, marker="complete")
final = run()
assert final["verified_publications"] == 4
module.append_collection(source, count=2, start_gid=1000005, pages=2, profile="small", collection="growth-append-1")
grouped = run()
assert grouped["verified_publications"] == 6 and grouped["verified_pages"] == final["verified_pages"] + 4
module.change(source, gid=1000005, generation=2)
final = run()
assert final["verified_publications"] == 6
assert (source / "growth-append-1/1000005/galleryinfo.txt").is_file()
report = module.verify(config=config_path, source=source, library=library, expected_manifest=source / module.MANIFEST_NAME, output=root / "verified.json")
assert report["artifacts"] == final["artifacts"]
from io import BytesIO
from PIL import Image
with Image.open(source / "1000001/0001.png") as image:
    encoded = BytesIO()
    image.save(encoded, format="JPEG", quality=70)
    altered = image.copy()
    altered.paste(255, (0, image.width, image.width, image.height))
    altered_encoded = BytesIO()
    altered.save(altered_encoded, format="JPEG", quality=90)
    altered.close()
expected_page = module.read_manifest(source / module.MANIFEST_NAME).galleries[0].expected.pages[0]
module._check_image(encoded.getvalue(), expected_page)
wrong_page = replace(expected_page, identity=module.page_identity(1000002, 0, 2))
try:
    module._check_image(encoded.getvalue(), wrong_page)
except ValueError as error:
    assert "different GID, page or generation" in str(error)
else:
    raise AssertionError("oracle accepted another gallery's image identity")
try:
    module._check_image(altered_encoded.getvalue(), expected_page)
except ValueError as error:
    assert "pixels differ" in str(error), str(error)
else:
    raise AssertionError("oracle accepted changed pixels outside the identity patch")
large = module.generate(root / "large", count=1, pages=1, profile="large")
with Image.open(root / "large/1000001/0001.png") as image:
    image.thumbnail((768, 1152), Image.Resampling.LANCZOS)
    for quality in (70, 90):
        encoded = BytesIO()
        image.save(encoded, format="JPEG", quality=quality)
        module._check_image(encoded.getvalue(), large.galleries[0].expected.pages[0], replace(module.DEFAULT_RENDER_CONTRACT, page_jpeg_quality=quality))
archive = library / "current" / report["artifacts"][0]["key"]
with archive.open("r+b") as stream:
    stream.seek(20)
    original = stream.read(1)
    stream.seek(20)
    stream.write(bytes([original[0] ^ 1]))
try:
    module.verify(config=config_path, source=source, library=library, expected_manifest=source / module.MANIFEST_NAME, output=root / "corrupt.json")
except ValueError as error:
    assert "CBZ differs" in str(error), str(error)
else:
    raise AssertionError("independent oracle accepted a corrupted archive")
assert not (root / "corrupt.json").exists()
print(json.dumps({"actual_galleries": 6, "actual_pages": report["verified_pages"], "oracle": "passed"}))
"""
    result = subprocess.run(
        [interpreter, "-c", program, str(_SCRIPT), str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.splitlines()[-1])["oracle"] == "passed"


@pytest.mark.parametrize(
    "short_side,preset", [(768, "canonical"), (256, "benchmark-low-cost")]
)
def test_real_ingest_resizes_mixed_large_pages_against_independent_raster(
    tmp_path: Path, short_side: int, preset: str
) -> None:
    program = r"""
import importlib.util
import json
import sys
from pathlib import Path

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import build_runtime
from h2hdb_ingest.scratch import DiskScratch

spec = importlib.util.spec_from_file_location("acceptance_resize_fixture", sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
root, short_side, preset = Path(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
source, library = root / "source", root / "library"
for child in ("current/acquisitions", "current/artwork", ".h2hdb-coordination"):
    (library / child).mkdir(parents=True)
manifest = module.generate(source, count=2, start_gid=1000019, pages=3, profile="mixed")
assert sum(p.width == 1024 for g in manifest.galleries for p in g.current.pages) == 2
config = IngestConfig(
    core=CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(root / "catalog.sqlite3"))),
    paths=IngestPathsConfig(download_path=source, library_path=library,
        page_render_workers=1, max_image_short_side=short_side, render_policy={"preset": preset}),
    resident=ResidentConfig(publication_batch_galleries=10, lease_seconds=30, heartbeat_seconds=5),
)
contract = module.resolve_render_contract(config.paths.model_dump(mode="json"))
assert contract.max_image_short_side == short_side
assert contract.page_jpeg_quality == (90 if preset == "canonical" else 70)
assert contract.resampler == ("lanczos" if preset == "canonical" else "bilinear")
with DiskScratch(library) as scratch, build_runtime(config, temporary_cleanup=scratch.cleanup_page) as runtime:
    runtime.database_admin.initialize()
    runtime.resident.initialize()
    assert runtime.resident.process_available(periodic_scan=True)
    report = module.verify_catalog(runtime.catalog, source=source, library=library,
        manifest=manifest, render_contract=contract)
assert report["verified_publications"] == 2 and report["verified_pages"] == 6
dimensions = [d for artifact in report["artifacts"] for d in artifact["page_dimensions"]]
assert dimensions.count([128, 192]) == 4
assert dimensions.count([short_side, short_side * 3 // 2]) == 2
assert all(max(a["page_rgb_rms"]) <= 16 for a in report["artifacts"])
assert report["render_contract"]["max_image_long_side"] == 8192
assert report["render_contract"]["format"] == "JPEG"
reader_config = root / "reader.json"
reader_config.write_text(config.core.model_dump_json(), encoding="utf-8")
render_config = root / "ingest.json"
render_config.write_text(config.model_dump_json(), encoding="utf-8")
assert module.main(["verify", "--config", str(reader_config), "--render-config", str(render_config),
    "--source", str(source), "--library", str(library), "--expected-manifest", str(source / module.MANIFEST_NAME),
    "--output", str(root / "oracle.json")]) == 0
saved = json.loads((root / "oracle.json").read_text())
assert saved["render_contract"] == report["render_contract"]
assert saved["artifacts"] == report["artifacts"]
print(json.dumps({"actual_galleries": 2, "actual_pages": 6,
    "large_source_pages": 2, "dimensions": dimensions,
    "maximum_rgb_rms": max(max(a["page_rgb_rms"]) for a in report["artifacts"]),
    "oracle": "passed"}))
"""
    result = subprocess.run(
        [
            _integration_python(),
            "-c",
            program,
            str(_SCRIPT),
            str(tmp_path),
            str(short_side),
            preset,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.splitlines()[-1])["oracle"] == "passed"
