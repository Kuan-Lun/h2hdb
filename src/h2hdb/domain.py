__all__ = [
    "CatalogSnapshot",
    "CatalogArtifact",
    "CatalogContributor",
    "CatalogPage",
    "CatalogPublishResult",
    "CatalogPublication",
    "CatalogPublicationSelection",
    "CatalogRevision",
    "CatalogSubject",
    "DownloadCandidateState",
    "GallerySourceFile",
    "GallerySourceRecord",
    "GalleryTag",
    "SchemaCompatibility",
]

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


def _validate_sha256(value: str, *, label: str) -> None:
    if len(value) != 64:
        raise ValueError(f"{label} must contain 64 hexadecimal characters")
    try:
        bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"{label} is not hexadecimal") from error


def _validate_leaf_name(value: str, *, label: str) -> None:
    if not value or value in {".", ".."}:
        raise ValueError(f"{label} must not be blank or a traversal segment")
    if "/" in value or "\\" in value or Path(value).name != value:
        raise ValueError(f"{label} must be a single leaf name")


@dataclass(frozen=True, slots=True)
class GallerySourceFile:
    name: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        _validate_leaf_name(self.name, label="Source file name")
        if len(self.name) > 255:
            raise ValueError("Source file name must not exceed 255 characters")
        if self.size_bytes < 0:
            raise ValueError("Source file size must not be negative")
        _validate_sha256(self.sha256, label="Source file SHA-256")


@dataclass(frozen=True, slots=True)
class GalleryTag:
    name: str
    value: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Gallery tag name must not be blank")
        if len(self.name) > 191 or len(self.value) > 191:
            raise ValueError(
                "Gallery tag name and value must not exceed 191 characters"
            )


@dataclass(frozen=True, slots=True)
class GallerySourceRecord:
    gallery_name: str
    gid: int
    title: str
    comment: str
    upload_account: str
    upload_time: datetime
    download_time: datetime
    modified_time: datetime
    tags: tuple[GalleryTag, ...]
    files: tuple[GallerySourceFile, ...]
    source_manifest_sha256: str
    content_sha256: str | None = None
    duplicate_of_gallery_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", tuple(self.tags))
        object.__setattr__(self, "files", tuple(self.files))
        _validate_leaf_name(self.gallery_name, label="Gallery name")
        if len(self.gallery_name) > 255:
            raise ValueError("Gallery name must not exceed 255 characters")
        if self.gid <= 0:
            raise ValueError("Gallery GID must be positive")
        _validate_sha256(
            self.source_manifest_sha256,
            label="Gallery source manifest SHA-256",
        )
        if self.content_sha256 is not None:
            _validate_sha256(self.content_sha256, label="Gallery content SHA-256")
        if self.duplicate_of_gallery_name is not None and self.content_sha256 is None:
            raise ValueError("A duplicate gallery must have a content SHA-256")
        file_names = [source_file.name for source_file in self.files]
        if len(file_names) != len(set(file_names)):
            raise ValueError("Gallery source files must have unique leaf names")
        tag_pairs = [(tag.name, tag.value) for tag in self.tags]
        if len(tag_pairs) != len(set(tag_pairs)):
            raise ValueError("Gallery tags must be unique")


@dataclass(frozen=True, slots=True)
class CatalogContributor:
    name: str
    role: str
    sort_as: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogSubject:
    name: str
    scheme: str | None = None
    code: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogArtifact:
    artifact_id: str
    # Neutral, user-facing download name. The physical/content-addressed path
    # belongs in ``location`` and must not be inferred from this value.
    name: str
    location: Path
    media_type: str
    size_bytes: int
    sha256: str
    modified_at: datetime

    def __post_init__(self) -> None:
        if self.size_bytes < 0:
            raise ValueError("Artifact size must not be negative")
        if len(self.sha256) != 64:
            raise ValueError("Artifact SHA-256 must contain 64 hexadecimal characters")
        try:
            bytes.fromhex(self.sha256)
        except ValueError as error:
            raise ValueError("Artifact SHA-256 is not hexadecimal") from error


@dataclass(frozen=True, slots=True)
class CatalogPublication:
    publication_id: str
    gid: int
    title: str
    source_title: str
    sort_title: str
    summary: str
    language: str
    published_at: datetime
    modified_at: datetime
    # The selected canonical source is part of every immutable revision.
    source_gallery_name: str
    contributors: tuple[CatalogContributor, ...] = field(default_factory=tuple)
    subjects: tuple[CatalogSubject, ...] = field(default_factory=tuple)
    artifacts: tuple[CatalogArtifact, ...] = field(default_factory=tuple)
    redownload_required: bool = False
    # A gallery can legitimately have no content digest when it contains no
    # non-galleryinfo content or every such file is excluded.
    content_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.gid <= 0:
            raise ValueError("Publication GID must be positive")
        if not self.publication_id:
            raise ValueError("Publication ID must not be blank")
        if not self.title:
            raise ValueError("Publication title must not be blank")
        _validate_leaf_name(
            self.source_gallery_name,
            label="Publication source gallery name",
        )
        if self.content_sha256 is not None:
            _validate_sha256(
                self.content_sha256,
                label="Publication content SHA-256",
            )


@dataclass(frozen=True, slots=True)
class CatalogPublicationSelection:
    """Select one canonical source record for the published projection.

    Canonical metadata deliberately does not belong here.  The core derives it
    from the matching :class:`GallerySourceRecord` in the same snapshot.
    """

    source_gallery_name: str
    artifacts: tuple[CatalogArtifact, ...] = field(default_factory=tuple)
    redownload_required: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifacts", tuple(self.artifacts))
        if not self.source_gallery_name:
            raise ValueError("Selected source gallery name must not be blank")


@dataclass(frozen=True, slots=True)
class CatalogSnapshot:
    galleries: tuple[GallerySourceRecord, ...]
    selections: tuple[CatalogPublicationSelection, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "galleries", tuple(self.galleries))
        object.__setattr__(self, "selections", tuple(self.selections))

        gallery_names = [gallery.gallery_name for gallery in self.galleries]
        if len(gallery_names) != len(set(gallery_names)):
            raise ValueError("Catalog snapshot contains a duplicate gallery name")

        galleries_by_name = {
            gallery.gallery_name: gallery for gallery in self.galleries
        }
        owner_by_content_sha256: dict[str, str] = {}
        for gallery in self.galleries:
            target_name = gallery.duplicate_of_gallery_name
            if target_name is None:
                if gallery.content_sha256 is not None:
                    previous_owner = owner_by_content_sha256.setdefault(
                        gallery.content_sha256,
                        gallery.gallery_name,
                    )
                    if previous_owner != gallery.gallery_name:
                        raise ValueError(
                            "Catalog snapshot has multiple owners for content SHA-256 "
                            f"{gallery.content_sha256}"
                        )
                continue
            if target_name == gallery.gallery_name:
                raise ValueError("A duplicate gallery cannot target itself")
            target = galleries_by_name.get(target_name)
            if target is None:
                raise ValueError(
                    f"Duplicate target gallery {target_name!r} is not in the snapshot"
                )
            if target.duplicate_of_gallery_name is not None:
                raise ValueError(
                    "A duplicate gallery must target the final content owner"
                )
            if target.content_sha256 != gallery.content_sha256:
                raise ValueError(
                    "A duplicate gallery and its target must have the same content "
                    "SHA-256"
                )

        selected_names = [
            selection.source_gallery_name for selection in self.selections
        ]
        if len(selected_names) != len(set(selected_names)):
            raise ValueError("Catalog snapshot contains a duplicate selected source")
        missing_names = set(selected_names).difference(galleries_by_name)
        if missing_names:
            raise ValueError(
                "Catalog selections reference absent canonical source galleries: "
                + ", ".join(repr(name) for name in sorted(missing_names))
            )
        selected_gids = [galleries_by_name[name].gid for name in selected_names]
        if len(selected_gids) != len(set(selected_gids)):
            raise ValueError(
                "Catalog snapshot contains a duplicate GID among selected sources"
            )
        artifact_ids = [
            artifact.artifact_id
            for selection in self.selections
            for artifact in selection.artifacts
        ]
        artifact_names = [
            artifact.name
            for selection in self.selections
            for artifact in selection.artifacts
        ]
        for label, values in (
            ("artifact ID", artifact_ids),
            ("artifact name", artifact_names),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"Catalog snapshot contains a duplicate {label}")


@dataclass(frozen=True, slots=True)
class CatalogRevision:
    revision: int
    published_at: datetime
    publication_count: int


@dataclass(frozen=True, slots=True)
class CatalogPublishResult:
    revision: CatalogRevision
    new_galleries: int
    changed_galleries: int
    removed_galleries: int

    def __post_init__(self) -> None:
        if (
            min(
                self.new_galleries,
                self.changed_galleries,
                self.removed_galleries,
            )
            < 0
        ):
            raise ValueError("Catalog publish counts must not be negative")


@dataclass(frozen=True, slots=True)
class CatalogPage:
    revision: CatalogRevision
    publications: tuple[CatalogPublication, ...]
    offset: int
    limit: int
    total: int


@dataclass(frozen=True, slots=True)
class DownloadCandidateState:
    gid: int
    cataloged: bool
    redownload_required: bool
    requested: bool


@dataclass(frozen=True, slots=True)
class SchemaCompatibility:
    database_version: int
    minimum_supported: int
    maximum_supported: int
