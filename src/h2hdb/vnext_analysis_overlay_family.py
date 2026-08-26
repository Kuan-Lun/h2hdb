"""Atomic BCNF storage protocols for analysis overlays and provenance.

File-decision shadows remain a sealed narrow family.  Gallery-scaled content
candidate, content-owner, and impacted-key facts are atomic base rows.
Impacted-key provenance is append-only: galleries are processed in strictly
increasing order, so the first gallery for a key is its immutable global-minimum
witness.
"""

from __future__ import annotations

__all__ = [
    "AnalysisContentOwnerCandidateShadowFamily",
    "AnalysisContentOwnerShadowFamily",
    "AnalysisFileHashDecisionShadowFamily",
    "AnalysisImpactedContentKeyFamily",
    "AnalysisImpactedContentProvenancePageReceipt",
    "AnalysisImpactedGidKeyFamily",
    "AnalysisImpactedGidProvenancePageReceipt",
    "apply_analysis_impacted_content_provenance_page",
    "apply_analysis_impacted_gid_provenance_page",
    "ensure_analysis_content_owner_candidate_shadow_family",
    "ensure_analysis_content_owner_shadow_family",
    "ensure_analysis_file_hash_decision_shadow_family",
    "load_analysis_content_owner_candidate_shadow_family",
    "load_analysis_content_owner_shadow_family",
    "load_analysis_file_hash_decision_shadow_family",
    "load_analysis_impacted_content_key_family",
    "load_analysis_impacted_content_provenance_page",
    "load_analysis_impacted_gid_key_family",
    "load_analysis_impacted_gid_provenance_page",
    "record_analysis_impacted_content_provenance",
    "record_analysis_impacted_content_provenance_page",
    "record_analysis_impacted_gid_provenance",
    "record_analysis_impacted_gid_provenance_page",
    "prepare_analysis_impacted_content_provenance_page",
    "prepare_analysis_impacted_gid_provenance_page",
    "require_exact_analysis_impacted_content_provenance_page",
    "require_exact_analysis_impacted_gid_provenance_page",
    "require_complete_analysis_impacted_content_keyspace",
    "require_complete_analysis_impacted_gid_keyspace",
]

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .vnext_analysis_family import (
    AnalysisFamilyCollisionError,
    AnalysisFamilyPartialError,
)
from .vnext_domains import (
    require_bool_byte,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)

_FILE_ANCHOR = "catalog_a_file_decision_shadow_anchors"
_FILE_OCCURRENCE = "catalog_a_file_decision_shadow_occurrences"
_FILE_ARTIST = "catalog_a_file_decision_shadow_artists"
_FILE_GALLERY_ARTIST_MAX = "catalog_a_file_decision_shadow_gallery_artist_max"
_FILE_SEAL = "catalog_a_file_decision_shadow_seals"

_CANDIDATE = "catalog_analysis_content_owner_candidate_shadows"
_OWNER = "catalog_analysis_content_owner_shadows"

_IMPACTED_CONTENT = "catalog_analysis_impacted_content"
_IMPACTED_CONTENT_PROVENANCE = "catalog_a_impacted_content_provenance"

_IMPACTED_GID = "catalog_analysis_impacted_gid"
_IMPACTED_GID_PROVENANCE = "catalog_a_impacted_gid_provenance"
_IMPACTED_GID_STORAGE = "catalog_analysis_impacted_gid_storage"
_IMPACTED_GID_PROVENANCE_STORAGE = "catalog_a_impacted_gid_provenance_storage"

_MAX_PROVENANCE_PAGE_ROWS = 256
_PROVENANCE_QUERY_LIMIT = _MAX_PROVENANCE_PAGE_ROWS + 1
_PROVENANCE_PAGE_TOKEN = object()


def _require_gallery_gid_entries(
    connector: Any,
    entries: Sequence[tuple[int, int]],
) -> None:
    """Validate every proposed GID against the immutable gallery-name chain."""

    if not entries:
        return
    gallery_ids = tuple(gallery for gallery, _gid in entries)
    placeholders = ", ".join("%s" for _gallery in gallery_ids)
    rows = connector.fetch_all(
        "SELECT access.gallery_id, name_gid.gid "
        "FROM catalog_gallery_source_name_accesses AS access "
        "JOIN catalog_source_gallery_name_gids AS name_gid "
        "ON name_gid.source_gallery_name = access.source_gallery_name "
        f"WHERE access.gallery_id IN ({placeholders}) "
        "ORDER BY access.gallery_id",
        gallery_ids,
    )
    expected = tuple(sorted(entries))
    if len(rows) != len(expected) or any(len(row) != 2 for row in rows):
        raise AnalysisFamilyPartialError(
            "impacted GID provenance has an incomplete gallery identity chain"
        )
    actual = tuple((row[0], row[1]) for row in rows)
    if actual != expected:
        raise AnalysisFamilyCollisionError(
            "impacted GID provenance disagrees with the gallery identity chain"
        )


@dataclass(frozen=True, slots=True)
class AnalysisFileHashDecisionShadowFamily:
    analysis_id: bytes
    file_sha256: bytes
    occurrence_count: int
    artist_count: int
    maximum_gallery_artist_count: int

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="file shadow analysis_id")
        require_digest32(self.file_sha256, field="file shadow file_sha256")
        require_int63(self.occurrence_count, field="file shadow occurrence_count")
        require_int63(self.artist_count, field="file shadow artist_count")
        require_int63(
            self.maximum_gallery_artist_count,
            field="file shadow maximum_gallery_artist_count",
        )

    @property
    def facts(self) -> tuple[int, int, int]:
        return (
            self.occurrence_count,
            self.artist_count,
            self.maximum_gallery_artist_count,
        )


@dataclass(frozen=True, slots=True)
class AnalysisContentOwnerCandidateShadowFamily:
    analysis_id: bytes
    gallery_id: int
    content_sha256: bytes
    prefer_not_already_uploaded: int
    title_scalar_count: int
    download_time: int

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="candidate shadow analysis_id")
        require_positive_int63(self.gallery_id, field="candidate shadow gallery_id")
        require_digest32(
            self.content_sha256,
            field="candidate shadow content_sha256",
        )
        require_bool_byte(
            self.prefer_not_already_uploaded,
            field="candidate shadow prefer_not_already_uploaded",
        )
        require_int63(
            self.title_scalar_count,
            field="candidate shadow title_scalar_count",
        )
        require_int63(self.download_time, field="candidate shadow download_time")

    @property
    def facts(self) -> tuple[bytes, int, int, int]:
        return (
            self.content_sha256,
            self.prefer_not_already_uploaded,
            self.title_scalar_count,
            self.download_time,
        )


@dataclass(frozen=True, slots=True)
class AnalysisContentOwnerShadowFamily:
    analysis_id: bytes
    content_sha256: bytes
    owner_gallery_id: int

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="owner shadow analysis_id")
        require_digest32(self.content_sha256, field="owner shadow content_sha256")
        require_positive_int63(
            self.owner_gallery_id,
            field="owner shadow owner_gallery_id",
        )


@dataclass(frozen=True, slots=True)
class AnalysisImpactedContentKeyFamily:
    analysis_id: bytes
    content_sha256: bytes
    witness_gallery_id: int

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="impacted content analysis_id")
        require_digest32(
            self.content_sha256,
            field="impacted content_sha256",
        )
        require_positive_int63(
            self.witness_gallery_id,
            field="impacted content witness_gallery_id",
        )


@dataclass(frozen=True, slots=True)
class AnalysisImpactedGidKeyFamily:
    analysis_id: bytes
    gid: int
    witness_gallery_id: int

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="impacted GID analysis_id")
        require_positive_int63(self.gid, field="impacted gid")
        require_positive_int63(
            self.witness_gallery_id,
            field="impacted GID witness_gallery_id",
        )


@dataclass(frozen=True, slots=True)
class AnalysisImpactedContentProvenancePageReceipt:
    """Opaque result of the one-read content-family page preflight."""

    analysis_id: bytes
    entries: tuple[tuple[int, bytes], ...]
    existing_witnesses: tuple[tuple[bytes, int], ...]
    _capability: object

    def __post_init__(self) -> None:
        if self._capability is not _PROVENANCE_PAGE_TOKEN:
            raise TypeError("content provenance page receipts are helper-issued")
        require_uuid16(self.analysis_id, field="impacted content analysis_id")

    @property
    def existing_keys(self) -> frozenset[bytes]:
        return frozenset(key for key, _witness in self.existing_witnesses)


@dataclass(frozen=True, slots=True)
class AnalysisImpactedGidProvenancePageReceipt:
    """Opaque result of the one-read GID-family page preflight."""

    analysis_id: bytes
    entries: tuple[tuple[int, int], ...]
    existing_witnesses: tuple[tuple[int, int], ...]
    _capability: object

    def __post_init__(self) -> None:
        if self._capability is not _PROVENANCE_PAGE_TOKEN:
            raise TypeError("GID provenance page receipts are helper-issued")
        require_uuid16(self.analysis_id, field="impacted GID analysis_id")

    @property
    def existing_keys(self) -> frozenset[int]:
        return frozenset(key for key, _witness in self.existing_witnesses)


def load_analysis_file_hash_decision_shadow_family(
    connector: Any,
    *,
    analysis_id: bytes,
    file_sha256: bytes,
) -> AnalysisFileHashDecisionShadowFamily | None:
    analysis = require_uuid16(analysis_id, field="file shadow analysis_id")
    digest = require_digest32(file_sha256, field="file shadow file_sha256")
    row = connector.fetch_one(
        "WITH family_keys(analysis_id, file_sha256) AS ("
        f"SELECT analysis_id, file_sha256 FROM {_FILE_ANCHOR} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_FILE_OCCURRENCE} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_FILE_ARTIST} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_FILE_GALLERY_ARTIST_MAX} "
        "WHERE analysis_id = %s AND file_sha256 = %s UNION "
        f"SELECT analysis_id, file_sha256 FROM {_FILE_SEAL} "
        "WHERE analysis_id = %s AND file_sha256 = %s) "
        "SELECT anchor.analysis_id, occurrence.analysis_id, "
        "occurrence.occurrence_count, artist.analysis_id, artist.artist_count, "
        "maximum.analysis_id, maximum.maximum_gallery_artist_count, seal.analysis_id "
        "FROM family_keys AS keyset "
        f"LEFT JOIN {_FILE_ANCHOR} AS anchor "
        "ON anchor.analysis_id = keyset.analysis_id "
        "AND anchor.file_sha256 = keyset.file_sha256 "
        f"LEFT JOIN {_FILE_OCCURRENCE} AS occurrence "
        "ON occurrence.analysis_id = keyset.analysis_id "
        "AND occurrence.file_sha256 = keyset.file_sha256 "
        f"LEFT JOIN {_FILE_ARTIST} AS artist "
        "ON artist.analysis_id = keyset.analysis_id "
        "AND artist.file_sha256 = keyset.file_sha256 "
        f"LEFT JOIN {_FILE_GALLERY_ARTIST_MAX} AS maximum "
        "ON maximum.analysis_id = keyset.analysis_id "
        "AND maximum.file_sha256 = keyset.file_sha256 "
        f"LEFT JOIN {_FILE_SEAL} AS seal "
        "ON seal.analysis_id = keyset.analysis_id "
        "AND seal.file_sha256 = keyset.file_sha256",
        (analysis, digest) * 5,
    )
    if not row:
        return None
    exact = tuple(row)
    if len(exact) != 8 or any(exact[index] != analysis for index in (0, 1, 3, 5, 7)):
        raise AnalysisFamilyPartialError("file-decision shadow family is partial")
    try:
        return AnalysisFileHashDecisionShadowFamily(
            analysis,
            digest,
            exact[2],
            exact[4],
            exact[6],
        )
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            "file-decision shadow family contains invalid facts"
        ) from error


def ensure_analysis_file_hash_decision_shadow_family(
    connector: Any,
    family: AnalysisFileHashDecisionShadowFamily,
) -> tuple[AnalysisFileHashDecisionShadowFamily, bool]:
    if not isinstance(family, AnalysisFileHashDecisionShadowFamily):
        raise TypeError("family must be AnalysisFileHashDecisionShadowFamily")
    existing = load_analysis_file_hash_decision_shadow_family(
        connector,
        analysis_id=family.analysis_id,
        file_sha256=family.file_sha256,
    )
    if existing is not None:
        if existing != family:
            raise AnalysisFamilyCollisionError("file-decision shadow replay changed")
        return existing, False
    key = (family.analysis_id, family.file_sha256)
    connector.execute(
        f"INSERT INTO {_FILE_ANCHOR} (analysis_id, file_sha256) VALUES (%s, %s)",
        key,
    )
    connector.execute(
        f"INSERT INTO {_FILE_OCCURRENCE} "
        "(analysis_id, file_sha256, occurrence_count) VALUES (%s, %s, %s)",
        (*key, family.occurrence_count),
    )
    connector.execute(
        f"INSERT INTO {_FILE_ARTIST} "
        "(analysis_id, file_sha256, artist_count) VALUES (%s, %s, %s)",
        (*key, family.artist_count),
    )
    connector.execute(
        f"INSERT INTO {_FILE_GALLERY_ARTIST_MAX} "
        "(analysis_id, file_sha256, maximum_gallery_artist_count) "
        "VALUES (%s, %s, %s)",
        (*key, family.maximum_gallery_artist_count),
    )
    connector.execute(
        f"INSERT INTO {_FILE_SEAL} (analysis_id, file_sha256) VALUES (%s, %s)",
        key,
    )
    return family, True


def load_analysis_content_owner_candidate_shadow_family(
    connector: Any,
    *,
    analysis_id: bytes,
    gallery_id: int,
) -> AnalysisContentOwnerCandidateShadowFamily | None:
    analysis = require_uuid16(analysis_id, field="candidate shadow analysis_id")
    gallery = require_positive_int63(gallery_id, field="candidate shadow gallery_id")
    row = connector.fetch_one(
        f"SELECT content_sha256, prefer_not_already_uploaded, "
        f"title_scalar_count, download_time FROM {_CANDIDATE} "
        "WHERE analysis_id = %s AND gallery_id = %s",
        (analysis, gallery),
    )
    if not row:
        return None
    exact = tuple(row)
    try:
        if len(exact) != 4:
            raise ValueError("content-candidate shadow row has an invalid shape")
        return AnalysisContentOwnerCandidateShadowFamily(
            analysis,
            gallery,
            exact[0],
            exact[1],
            exact[2],
            exact[3],
        )
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            "content-candidate shadow family contains invalid facts"
        ) from error


def ensure_analysis_content_owner_candidate_shadow_family(
    connector: Any,
    family: AnalysisContentOwnerCandidateShadowFamily,
) -> tuple[AnalysisContentOwnerCandidateShadowFamily, bool]:
    if not isinstance(family, AnalysisContentOwnerCandidateShadowFamily):
        raise TypeError("family must be AnalysisContentOwnerCandidateShadowFamily")
    existing = load_analysis_content_owner_candidate_shadow_family(
        connector,
        analysis_id=family.analysis_id,
        gallery_id=family.gallery_id,
    )
    if existing is not None:
        if existing != family:
            raise AnalysisFamilyCollisionError(
                "content-candidate shadow replay changed"
            )
        return existing, False
    connector.execute(
        f"INSERT INTO {_CANDIDATE} "
        "(analysis_id, gallery_id, content_sha256, "
        "prefer_not_already_uploaded, title_scalar_count, download_time) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            family.analysis_id,
            family.gallery_id,
            family.content_sha256,
            family.prefer_not_already_uploaded,
            family.title_scalar_count,
            family.download_time,
        ),
    )
    return family, True


def load_analysis_content_owner_shadow_family(
    connector: Any,
    *,
    analysis_id: bytes,
    content_sha256: bytes,
) -> AnalysisContentOwnerShadowFamily | None:
    analysis = require_uuid16(analysis_id, field="owner shadow analysis_id")
    content = require_digest32(content_sha256, field="owner shadow content_sha256")
    row = connector.fetch_one(
        f"SELECT owner_gallery_id FROM {_OWNER} "
        "WHERE analysis_id = %s AND content_sha256 = %s",
        (analysis, content),
    )
    if not row:
        return None
    exact = tuple(row)
    try:
        if len(exact) != 1:
            raise ValueError("content-owner shadow row has an invalid shape")
        return AnalysisContentOwnerShadowFamily(analysis, content, exact[0])
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            "content-owner shadow family contains invalid facts"
        ) from error


def ensure_analysis_content_owner_shadow_family(
    connector: Any,
    family: AnalysisContentOwnerShadowFamily,
) -> tuple[AnalysisContentOwnerShadowFamily, bool]:
    if not isinstance(family, AnalysisContentOwnerShadowFamily):
        raise TypeError("family must be AnalysisContentOwnerShadowFamily")
    existing = load_analysis_content_owner_shadow_family(
        connector,
        analysis_id=family.analysis_id,
        content_sha256=family.content_sha256,
    )
    if existing is not None:
        if existing != family:
            raise AnalysisFamilyCollisionError("content-owner shadow replay changed")
        return existing, False
    connector.execute(
        f"INSERT INTO {_OWNER} "
        "(analysis_id, content_sha256, owner_gallery_id) VALUES (%s, %s, %s)",
        (family.analysis_id, family.content_sha256, family.owner_gallery_id),
    )
    return family, True


def _load_impacted_key_family(
    connector: Any,
    *,
    analysis_id: bytes,
    key: bytes | int,
    key_column: str,
    impacted_table: str,
    provenance_table: str,
    label: str,
) -> tuple[int, int] | None:
    row = connector.fetch_one(
        f"SELECT impacted.witness_gallery_id, (SELECT MIN(provenance.gallery_id) "
        f"FROM {provenance_table} AS provenance "
        "WHERE provenance.analysis_id = impacted.analysis_id "
        f"AND provenance.{key_column} = impacted.{key_column}) "
        f"FROM {impacted_table} AS impacted WHERE impacted.analysis_id = %s "
        f"AND impacted.{key_column} = %s",
        (analysis_id, key),
    )
    if not row:
        return None
    exact = tuple(row)
    if len(exact) != 2:
        raise AnalysisFamilyPartialError(f"{label} key row has an invalid shape")
    try:
        witness = require_positive_int63(
            exact[0],
            field=f"{label} witness_gallery_id",
        )
        minimum = require_positive_int63(
            exact[1],
            field=f"{label} minimum provenance gallery_id",
        )
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            f"{label} key family contains invalid provenance"
        ) from error
    if witness != minimum:
        raise AnalysisFamilyCollisionError(
            f"{label} witness is not the global minimum provenance gallery"
        )
    return witness, minimum


def load_analysis_impacted_content_key_family(
    connector: Any,
    *,
    analysis_id: bytes,
    content_sha256: bytes,
) -> AnalysisImpactedContentKeyFamily | None:
    analysis = require_uuid16(analysis_id, field="impacted content analysis_id")
    content = require_digest32(content_sha256, field="impacted content_sha256")
    loaded = _load_impacted_key_family(
        connector,
        analysis_id=analysis,
        key=content,
        key_column="content_sha256",
        impacted_table=_IMPACTED_CONTENT,
        provenance_table=_IMPACTED_CONTENT_PROVENANCE,
        label="impacted content",
    )
    if loaded is None:
        return None
    return AnalysisImpactedContentKeyFamily(analysis, content, loaded[0])


def load_analysis_impacted_gid_key_family(
    connector: Any,
    *,
    analysis_id: bytes,
    gid: int,
) -> AnalysisImpactedGidKeyFamily | None:
    analysis = require_uuid16(analysis_id, field="impacted GID analysis_id")
    exact_gid = require_positive_int63(gid, field="impacted gid")
    row = connector.fetch_one(
        f"SELECT base.analysis_id, (SELECT MIN(provenance.gallery_id) "
        f"FROM {_IMPACTED_GID_PROVENANCE} AS provenance "
        "WHERE provenance.analysis_id = base.analysis_id "
        f"AND provenance.gid = base.gid) FROM {_IMPACTED_GID_STORAGE} AS base "
        "WHERE base.analysis_id = %s AND base.gid = %s",
        (analysis, exact_gid),
    )
    if not row:
        orphan = connector.fetch_one(
            f"SELECT 1 FROM {_IMPACTED_GID_PROVENANCE} "
            "WHERE analysis_id = %s AND gid = %s LIMIT 1",
            (analysis, exact_gid),
        )
        if orphan:
            raise AnalysisFamilyPartialError(
                "impacted GID provenance has no frozen key storage"
            )
        return None
    if len(row) != 2 or row[0] != analysis or row[1] is None:
        raise AnalysisFamilyPartialError(
            "impacted GID key storage has no complete provenance"
        )
    witness = require_positive_int63(row[1], field="impacted GID witness_gallery_id")
    return AnalysisImpactedGidKeyFamily(analysis, exact_gid, witness)


def _record_impacted_provenance(
    connector: Any,
    *,
    analysis_id: bytes,
    gallery_id: int,
    key: bytes | int,
    key_column: str,
    impacted_table: str,
    provenance_table: str,
    existing_witness: int | None,
    label: str,
) -> tuple[int, bool]:
    if existing_witness is not None:
        exact = connector.fetch_one(
            f"SELECT 1 FROM {provenance_table} WHERE analysis_id = %s "
            f"AND gallery_id = %s AND {key_column} = %s",
            (analysis_id, gallery_id, key),
        )
        if exact:
            if tuple(exact) != (1,):
                raise AnalysisFamilyCollisionError(
                    f"{label} provenance replay marker is malformed"
                )
            return existing_witness, False
        if gallery_id <= existing_witness:
            raise AnalysisFamilyCollisionError(
                f"{label} provenance is not strictly after its stable witness"
            )
        connector.execute(
            f"INSERT INTO {provenance_table} "
            f"(analysis_id, gallery_id, {key_column}) VALUES (%s, %s, %s)",
            (analysis_id, gallery_id, key),
        )
        return existing_witness, False

    connector.execute(
        f"INSERT INTO {provenance_table} "
        f"(analysis_id, gallery_id, {key_column}) VALUES (%s, %s, %s)",
        (analysis_id, gallery_id, key),
    )
    connector.execute(
        f"INSERT INTO {impacted_table} "
        f"(analysis_id, {key_column}, witness_gallery_id) VALUES (%s, %s, %s)",
        (analysis_id, key, gallery_id),
    )
    return gallery_id, True


def record_analysis_impacted_content_provenance(
    connector: Any,
    *,
    analysis_id: bytes,
    gallery_id: int,
    content_sha256: bytes,
) -> tuple[AnalysisImpactedContentKeyFamily, bool]:
    analysis = require_uuid16(analysis_id, field="impacted content analysis_id")
    gallery = require_positive_int63(
        gallery_id,
        field="impacted content provenance gallery_id",
    )
    content = require_digest32(content_sha256, field="impacted content_sha256")
    existing = load_analysis_impacted_content_key_family(
        connector,
        analysis_id=analysis,
        content_sha256=content,
    )
    witness, created = _record_impacted_provenance(
        connector,
        analysis_id=analysis,
        gallery_id=gallery,
        key=content,
        key_column="content_sha256",
        impacted_table=_IMPACTED_CONTENT,
        provenance_table=_IMPACTED_CONTENT_PROVENANCE,
        existing_witness=None if existing is None else existing.witness_gallery_id,
        label="impacted content",
    )
    return AnalysisImpactedContentKeyFamily(analysis, content, witness), created


def record_analysis_impacted_gid_provenance(
    connector: Any,
    *,
    analysis_id: bytes,
    gallery_id: int,
    gid: int,
) -> tuple[AnalysisImpactedGidKeyFamily, bool]:
    analysis = require_uuid16(analysis_id, field="impacted GID analysis_id")
    gallery = require_positive_int63(
        gallery_id,
        field="impacted GID provenance gallery_id",
    )
    exact_gid = require_positive_int63(gid, field="impacted gid")
    _require_gallery_gid_entries(connector, ((gallery, exact_gid),))
    existing = load_analysis_impacted_gid_key_family(
        connector,
        analysis_id=analysis,
        gid=exact_gid,
    )
    exact = connector.fetch_one(
        f"SELECT gid FROM {_IMPACTED_GID_PROVENANCE} "
        "WHERE analysis_id = %s AND gallery_id = %s",
        (analysis, gallery),
    )
    if exact:
        if exact != (exact_gid,):
            raise AnalysisFamilyCollisionError(
                "impacted GID gallery derives a conflicting GID"
            )
        if existing is None:
            raise AnalysisFamilyPartialError(
                "impacted GID provenance exists without frozen key storage"
            )
        return existing, False
    if existing is not None and gallery <= existing.witness_gallery_id:
        raise AnalysisFamilyCollisionError(
            "impacted GID provenance is not strictly after its stable witness"
        )
    connector.execute(
        f"INSERT INTO {_IMPACTED_GID_PROVENANCE_STORAGE} "
        "(analysis_id, gallery_id) VALUES (%s, %s)",
        (analysis, gallery),
    )
    created = existing is None
    witness = gallery if existing is None else existing.witness_gallery_id
    if created:
        connector.execute(
            f"INSERT INTO {_IMPACTED_GID_STORAGE} (analysis_id, gid) VALUES (%s, %s)",
            (analysis, exact_gid),
        )
    return AnalysisImpactedGidKeyFamily(analysis, exact_gid, witness), created


def _preflight_impacted_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    entries: Sequence[tuple[int, bytes | int]],
    key_column: str,
    impacted_table: str,
    provenance_table: str,
    label: str,
) -> dict[bytes | int, int]:
    """Load every existing proposed key family with one bounded set query."""

    if not entries:
        return {}
    keys = tuple(sorted({key for _gallery, key in entries}))
    if len(keys) > _MAX_PROVENANCE_PAGE_ROWS:
        raise ValueError(f"{label} page has too many distinct keys")
    key_placeholders = ", ".join("%s" for _key in keys)
    page_start = entries[0][0]
    parameters: list[Any] = [page_start, analysis_id, *keys, _PROVENANCE_QUERY_LIMIT]
    rows = connector.fetch_all(
        f"SELECT impacted.{key_column}, impacted.analysis_id, "
        "impacted.witness_gallery_id, "
        "witness_provenance.analysis_id, CASE WHEN NOT EXISTS ("
        f"SELECT 1 FROM {provenance_table} AS earlier "
        "WHERE earlier.analysis_id = impacted.analysis_id "
        f"AND earlier.{key_column} = impacted.{key_column} "
        "AND earlier.gallery_id < impacted.witness_gallery_id"
        ") THEN 1 ELSE 0 END, CASE WHEN EXISTS ("
        f"SELECT 1 FROM {provenance_table} AS page_or_future "
        "WHERE page_or_future.analysis_id = impacted.analysis_id "
        f"AND page_or_future.{key_column} = impacted.{key_column} "
        "AND page_or_future.gallery_id >= %s"
        f") THEN 1 ELSE 0 END FROM {impacted_table} AS impacted "
        f"LEFT JOIN {provenance_table} AS witness_provenance "
        "ON witness_provenance.analysis_id = impacted.analysis_id "
        f"AND witness_provenance.{key_column} = impacted.{key_column} "
        "AND witness_provenance.gallery_id = impacted.witness_gallery_id "
        "WHERE impacted.analysis_id = %s "
        f"AND impacted.{key_column} IN ({key_placeholders}) "
        f"ORDER BY impacted.{key_column} LIMIT %s",
        tuple(parameters),
    )
    existing: dict[bytes | int, int] = {}
    for row in rows:
        if len(row) != 6:
            raise AnalysisFamilyCollisionError(
                f"{label} preflight family has an invalid shape"
            )
        key = row[0]
        if key not in keys:
            raise AnalysisFamilyCollisionError(
                f"{label} preflight escaped its proposed key set"
            )
        if row[1] != analysis_id or row[3] != analysis_id:
            raise AnalysisFamilyPartialError(f"{label} key family is partial")
        try:
            witness = require_positive_int63(
                row[2],
                field=f"{label} preflight witness_gallery_id",
            )
        except (TypeError, ValueError) as error:
            raise AnalysisFamilyPartialError(
                f"{label} key family has no valid witness"
            ) from error
        if row[4] != 1:
            raise AnalysisFamilyCollisionError(
                f"{label} witness is not the global minimum provenance gallery"
            )
        if row[5] != 0:
            raise AnalysisFamilyCollisionError(
                f"{label} fresh page overlaps existing or future provenance"
            )
        existing[key] = witness
    return existing


def _apply_impacted_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    entries: Sequence[tuple[int, bytes | int]],
    existing_witnesses: Sequence[tuple[bytes | int, int]],
    key_column: str,
    impacted_table: str,
    provenance_table: str,
    label: str,
) -> None:
    normalized = tuple(entries)
    existing = dict(existing_witnesses)
    for gallery, key in normalized:
        witness = existing.get(key)
        if witness is None:
            connector.execute(
                f"INSERT INTO {provenance_table} "
                f"(analysis_id, gallery_id, {key_column}) VALUES (%s, %s, %s)",
                (analysis_id, gallery, key),
            )
            connector.execute(
                f"INSERT INTO {impacted_table} "
                f"(analysis_id, {key_column}, witness_gallery_id) "
                "VALUES (%s, %s, %s)",
                (analysis_id, key, gallery),
            )
            existing[key] = gallery
            continue
        if gallery <= witness:
            raise AnalysisFamilyCollisionError(
                f"{label} provenance is not strictly after its stable witness"
            )
        connector.execute(
            f"INSERT INTO {provenance_table} "
            f"(analysis_id, gallery_id, {key_column}) VALUES (%s, %s, %s)",
            (analysis_id, gallery, key),
        )


def prepare_analysis_impacted_content_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    entries: Sequence[tuple[int, bytes]],
) -> AnalysisImpactedContentProvenancePageReceipt:
    analysis = require_uuid16(analysis_id, field="impacted content analysis_id")
    normalized = tuple(
        (
            require_positive_int63(gallery, field="content provenance gallery_id"),
            require_digest32(content, field="content provenance content_sha256"),
        )
        for gallery, content in entries
    )
    if len(normalized) > _MAX_PROVENANCE_PAGE_ROWS:
        raise ValueError("impacted content provenance exceeds one bounded page")
    if tuple(sorted(set(normalized))) != normalized:
        raise ValueError("impacted content provenance is not an exact ordered set")
    existing = _preflight_impacted_provenance_page(
        connector,
        analysis_id=analysis,
        entries=normalized,
        key_column="content_sha256",
        impacted_table=_IMPACTED_CONTENT,
        provenance_table=_IMPACTED_CONTENT_PROVENANCE,
        label="impacted content",
    )
    return AnalysisImpactedContentProvenancePageReceipt(
        analysis,
        normalized,
        tuple(
            (
                require_digest32(key, field="preflight content_sha256"),
                witness,
            )
            for key, witness in sorted(existing.items())
        ),
        _PROVENANCE_PAGE_TOKEN,
    )


def apply_analysis_impacted_content_provenance_page(
    connector: Any,
    receipt: AnalysisImpactedContentProvenancePageReceipt,
) -> None:
    if type(receipt) is not AnalysisImpactedContentProvenancePageReceipt:
        raise TypeError("receipt must be an exact content provenance page receipt")
    receipt.__post_init__()
    _apply_impacted_provenance_page(
        connector,
        analysis_id=receipt.analysis_id,
        entries=receipt.entries,
        existing_witnesses=receipt.existing_witnesses,
        key_column="content_sha256",
        impacted_table=_IMPACTED_CONTENT,
        provenance_table=_IMPACTED_CONTENT_PROVENANCE,
        label="impacted content",
    )


def record_analysis_impacted_content_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    entries: Sequence[tuple[int, bytes]],
) -> None:
    apply_analysis_impacted_content_provenance_page(
        connector,
        prepare_analysis_impacted_content_provenance_page(
            connector,
            analysis_id=analysis_id,
            entries=entries,
        ),
    )


def prepare_analysis_impacted_gid_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    entries: Sequence[tuple[int, int]],
) -> AnalysisImpactedGidProvenancePageReceipt:
    analysis = require_uuid16(analysis_id, field="impacted GID analysis_id")
    normalized = tuple(
        (
            require_positive_int63(gallery, field="GID provenance gallery_id"),
            require_positive_int63(gid, field="provenance gid"),
        )
        for gallery, gid in entries
    )
    if len(normalized) > _MAX_PROVENANCE_PAGE_ROWS:
        raise ValueError("impacted GID provenance exceeds one bounded page")
    if tuple(sorted(set(normalized))) != normalized:
        raise ValueError("impacted GID provenance is not an exact ordered set")
    if len({gallery for gallery, _gid in normalized}) != len(normalized):
        raise ValueError(
            "one impacted GID provenance gallery cannot derive multiple GIDs"
        )
    _require_gallery_gid_entries(connector, normalized)
    existing = _preflight_impacted_provenance_page(
        connector,
        analysis_id=analysis,
        entries=normalized,
        key_column="gid",
        impacted_table=_IMPACTED_GID,
        provenance_table=_IMPACTED_GID_PROVENANCE,
        label="impacted GID",
    )
    return AnalysisImpactedGidProvenancePageReceipt(
        analysis,
        normalized,
        tuple(
            (
                require_positive_int63(key, field="preflight gid"),
                witness,
            )
            for key, witness in sorted(existing.items())
        ),
        _PROVENANCE_PAGE_TOKEN,
    )


def apply_analysis_impacted_gid_provenance_page(
    connector: Any,
    receipt: AnalysisImpactedGidProvenancePageReceipt,
) -> None:
    if type(receipt) is not AnalysisImpactedGidProvenancePageReceipt:
        raise TypeError("receipt must be an exact GID provenance page receipt")
    receipt.__post_init__()
    if receipt.entries:
        # The physical relation owns gallery uniqueness.  Preflight the whole
        # bounded page there so a collision is rejected before any row is written.
        gallery_ids = tuple(gallery for gallery, _gid in receipt.entries)
        placeholders = ", ".join("%s" for _gallery in gallery_ids)
        prior = connector.fetch_one(
            f"SELECT gallery_id FROM {_IMPACTED_GID_PROVENANCE_STORAGE} "
            "WHERE analysis_id = %s "
            f"AND gallery_id IN ({placeholders}) "
            "ORDER BY gallery_id LIMIT 1",
            (receipt.analysis_id, *gallery_ids),
        )
        if prior:
            raise AnalysisFamilyCollisionError(
                "impacted GID fresh page overlaps existing provenance"
            )
    existing = dict(receipt.existing_witnesses)
    for gallery, gid in receipt.entries:
        witness = existing.get(gid)
        if witness is not None and gallery <= witness:
            raise AnalysisFamilyCollisionError(
                "impacted GID provenance is not strictly after its stable witness"
            )
        connector.execute(
            f"INSERT INTO {_IMPACTED_GID_PROVENANCE_STORAGE} "
            "(analysis_id, gallery_id) VALUES (%s, %s)",
            (receipt.analysis_id, gallery),
        )
        if witness is None:
            connector.execute(
                f"INSERT INTO {_IMPACTED_GID_STORAGE} (analysis_id, gid) "
                "VALUES (%s, %s)",
                (receipt.analysis_id, gid),
            )
            existing[gid] = gallery


def record_analysis_impacted_gid_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    entries: Sequence[tuple[int, int]],
) -> None:
    apply_analysis_impacted_gid_provenance_page(
        connector,
        prepare_analysis_impacted_gid_provenance_page(
            connector,
            analysis_id=analysis_id,
            entries=entries,
        ),
    )


def _load_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    after_gallery_id: int | None,
    through_gallery_id: int | None,
    key_column: str,
    impacted_table: str,
    provenance_table: str,
    key_validator: Any,
    label: str,
) -> tuple[tuple[int, bytes | int], ...]:
    predicates = ["analysis_id = %s"]
    parameters: list[Any] = [analysis_id]
    if after_gallery_id is not None:
        predicates.append("gallery_id > %s")
        parameters.append(after_gallery_id)
    if through_gallery_id is not None:
        predicates.append("gallery_id <= %s")
        parameters.append(through_gallery_id)
    parameters.append(_PROVENANCE_QUERY_LIMIT)
    rows = connector.fetch_all(
        f"SELECT provenance.gallery_id, provenance.{key_column}, "
        "impacted.analysis_id, impacted.witness_gallery_id, "
        "witness_provenance.analysis_id, "
        "CASE WHEN NOT EXISTS ("
        f"SELECT 1 FROM {provenance_table} AS earlier "
        "WHERE earlier.analysis_id = provenance.analysis_id "
        f"AND earlier.{key_column} = provenance.{key_column} "
        "AND earlier.gallery_id < impacted.witness_gallery_id"
        ") THEN 1 ELSE 0 END "
        f"FROM {provenance_table} AS provenance "
        f"LEFT JOIN {impacted_table} AS impacted "
        "ON impacted.analysis_id = provenance.analysis_id "
        f"AND impacted.{key_column} = provenance.{key_column} "
        f"LEFT JOIN {provenance_table} AS witness_provenance "
        "ON witness_provenance.analysis_id = provenance.analysis_id "
        f"AND witness_provenance.{key_column} = provenance.{key_column} "
        "AND witness_provenance.gallery_id = impacted.witness_gallery_id WHERE "
        + " AND ".join(
            predicate.replace("analysis_id", "provenance.analysis_id").replace(
                "gallery_id", "provenance.gallery_id"
            )
            for predicate in predicates
        )
        + f" ORDER BY provenance.gallery_id, provenance.{key_column} LIMIT %s",
        tuple(parameters),
    )
    normalized: list[tuple[int, bytes | int]] = []
    previous: tuple[int, bytes | int] | None = None
    for row in rows:
        if len(row) != 6:
            raise AnalysisFamilyCollisionError(
                f"{label} provenance page has an invalid joined shape"
            )
        raw_gallery, raw_key = row[:2]
        gallery = require_positive_int63(
            raw_gallery,
            field=f"{label} provenance gallery_id",
        )
        key = key_validator(raw_key)
        if row[2] != analysis_id or row[4] != analysis_id:
            raise AnalysisFamilyPartialError(
                f"{label} provenance references a partial key family"
            )
        try:
            witness = require_positive_int63(
                row[3],
                field=f"{label} provenance witness_gallery_id",
            )
        except (TypeError, ValueError) as error:
            raise AnalysisFamilyPartialError(
                f"{label} provenance has no valid witness"
            ) from error
        if row[5] != 1 or witness > gallery:
            raise AnalysisFamilyCollisionError(
                f"{label} witness is not the global minimum provenance gallery"
            )
        current = (gallery, key)
        if previous is not None and current <= previous:
            raise AnalysisFamilyCollisionError(
                f"{label} provenance page is not strictly ordered"
            )
        normalized.append(current)
        previous = current
    return tuple(normalized)


def load_analysis_impacted_content_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    after_gallery_id: int | None,
    through_gallery_id: int | None,
) -> tuple[tuple[int, bytes], ...]:
    analysis = require_uuid16(analysis_id, field="impacted content analysis_id")
    after = (
        None
        if after_gallery_id is None
        else require_positive_int63(after_gallery_id, field="content page after")
    )
    through = (
        None
        if through_gallery_id is None
        else require_positive_int63(through_gallery_id, field="content page through")
    )
    if after is not None and through is not None and through <= after:
        raise ValueError("content provenance page bounds are not increasing")
    rows = _load_provenance_page(
        connector,
        analysis_id=analysis,
        after_gallery_id=after,
        through_gallery_id=through,
        key_column="content_sha256",
        impacted_table=_IMPACTED_CONTENT,
        provenance_table=_IMPACTED_CONTENT_PROVENANCE,
        key_validator=lambda value: require_digest32(
            value,
            field="impacted content provenance content_sha256",
        ),
        label="impacted content",
    )
    return tuple((gallery, key) for gallery, key in rows if isinstance(key, bytes))


def load_analysis_impacted_gid_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    after_gallery_id: int | None,
    through_gallery_id: int | None,
) -> tuple[tuple[int, int], ...]:
    analysis = require_uuid16(analysis_id, field="impacted GID analysis_id")
    after = (
        None
        if after_gallery_id is None
        else require_positive_int63(after_gallery_id, field="GID page after")
    )
    through = (
        None
        if through_gallery_id is None
        else require_positive_int63(through_gallery_id, field="GID page through")
    )
    if after is not None and through is not None and through <= after:
        raise ValueError("GID provenance page bounds are not increasing")
    rows = _load_provenance_page(
        connector,
        analysis_id=analysis,
        after_gallery_id=after,
        through_gallery_id=through,
        key_column="gid",
        impacted_table=_IMPACTED_GID,
        provenance_table=_IMPACTED_GID_PROVENANCE,
        key_validator=lambda value: require_positive_int63(
            value,
            field="impacted provenance gid",
        ),
        label="impacted GID",
    )
    return tuple((gallery, key) for gallery, key in rows if isinstance(key, int))


def _require_exact_provenance_page(
    *,
    actual: Sequence[tuple[int, bytes | int]],
    expected: Sequence[tuple[int, bytes | int]],
    label: str,
) -> None:
    if len(expected) > _MAX_PROVENANCE_PAGE_ROWS:
        raise ValueError(f"{label} expected provenance exceeds one bounded page")
    if tuple(actual) != tuple(expected):
        raise AnalysisFamilyCollisionError(
            f"{label} replay provenance differs from the committed exact page"
        )


def require_exact_analysis_impacted_content_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    after_gallery_id: int | None,
    through_gallery_id: int | None,
    expected: Sequence[tuple[int, bytes]],
) -> None:
    normalized = tuple(
        (
            require_positive_int63(gallery, field="expected content gallery_id"),
            require_digest32(content, field="expected content_sha256"),
        )
        for gallery, content in expected
    )
    if tuple(sorted(set(normalized))) != normalized:
        raise ValueError("expected content provenance is not an exact ordered set")
    actual = load_analysis_impacted_content_provenance_page(
        connector,
        analysis_id=analysis_id,
        after_gallery_id=after_gallery_id,
        through_gallery_id=through_gallery_id,
    )
    _require_exact_provenance_page(
        actual=actual,
        expected=normalized,
        label="impacted content",
    )


def require_exact_analysis_impacted_gid_provenance_page(
    connector: Any,
    *,
    analysis_id: bytes,
    after_gallery_id: int | None,
    through_gallery_id: int | None,
    expected: Sequence[tuple[int, int]],
) -> None:
    normalized = tuple(
        (
            require_positive_int63(gallery, field="expected GID gallery_id"),
            require_positive_int63(gid, field="expected gid"),
        )
        for gallery, gid in expected
    )
    if tuple(sorted(set(normalized))) != normalized:
        raise ValueError("expected GID provenance is not an exact ordered set")
    actual = load_analysis_impacted_gid_provenance_page(
        connector,
        analysis_id=analysis_id,
        after_gallery_id=after_gallery_id,
        through_gallery_id=through_gallery_id,
    )
    _require_exact_provenance_page(
        actual=actual,
        expected=normalized,
        label="impacted GID",
    )


def _require_complete_impacted_keyspace(
    connector: Any,
    *,
    analysis_id: bytes,
    key_column: str,
    impacted_table: str,
    provenance_table: str,
    label: str,
) -> None:
    violation = connector.fetch_one(
        "SELECT 1 FROM ("
        f"SELECT impacted.{key_column} FROM {impacted_table} AS impacted "
        "WHERE impacted.analysis_id = %s AND (NOT EXISTS ("
        f"SELECT 1 FROM {provenance_table} AS provenance "
        "WHERE provenance.analysis_id = impacted.analysis_id "
        f"AND provenance.{key_column} = impacted.{key_column}) OR NOT EXISTS ("
        f"SELECT 1 FROM {provenance_table} AS witness_provenance "
        "WHERE witness_provenance.analysis_id = impacted.analysis_id "
        f"AND witness_provenance.{key_column} = impacted.{key_column} "
        "AND witness_provenance.gallery_id = impacted.witness_gallery_id) OR EXISTS ("
        f"SELECT 1 FROM {provenance_table} AS earlier "
        "WHERE earlier.analysis_id = impacted.analysis_id "
        f"AND earlier.{key_column} = impacted.{key_column} "
        "AND earlier.gallery_id < impacted.witness_gallery_id)) UNION ALL "
        f"SELECT provenance.{key_column} FROM {provenance_table} AS provenance "
        "WHERE provenance.analysis_id = %s AND NOT EXISTS ("
        f"SELECT 1 FROM {impacted_table} AS impacted "
        "WHERE impacted.analysis_id = provenance.analysis_id "
        f"AND impacted.{key_column} = provenance.{key_column})"
        ") AS violations LIMIT 1",
        (analysis_id,) * 2,
    )
    if violation:
        raise AnalysisFamilyPartialError(
            f"{label} terminal keyspace contains a partial or nonminimum family"
        )


def require_complete_analysis_impacted_content_keyspace(
    connector: Any,
    *,
    analysis_id: bytes,
) -> None:
    analysis = require_uuid16(analysis_id, field="impacted content analysis_id")
    _require_complete_impacted_keyspace(
        connector,
        analysis_id=analysis,
        key_column="content_sha256",
        impacted_table=_IMPACTED_CONTENT,
        provenance_table=_IMPACTED_CONTENT_PROVENANCE,
        label="impacted content",
    )


def require_complete_analysis_impacted_gid_keyspace(
    connector: Any,
    *,
    analysis_id: bytes,
) -> None:
    analysis = require_uuid16(analysis_id, field="impacted GID analysis_id")
    physical_violation = connector.fetch_one(
        f"SELECT 1 FROM {_IMPACTED_GID_STORAGE} AS stored "
        "WHERE stored.analysis_id = %s AND NOT EXISTS ("
        f"SELECT 1 FROM {_IMPACTED_GID} AS projected "
        "WHERE projected.analysis_id = stored.analysis_id "
        "AND projected.gid = stored.gid) UNION ALL "
        f"SELECT 1 FROM {_IMPACTED_GID_PROVENANCE_STORAGE} AS stored "
        "WHERE stored.analysis_id = %s AND NOT EXISTS ("
        f"SELECT 1 FROM {_IMPACTED_GID_PROVENANCE} AS projected "
        "WHERE projected.analysis_id = stored.analysis_id "
        "AND projected.gallery_id = stored.gallery_id) LIMIT 1",
        (analysis, analysis),
    )
    if physical_violation:
        raise AnalysisFamilyPartialError(
            "impacted GID terminal keyspace contains orphan physical storage"
        )
    _require_complete_impacted_keyspace(
        connector,
        analysis_id=analysis,
        key_column="gid",
        impacted_table=_IMPACTED_GID,
        provenance_table=_IMPACTED_GID_PROVENANCE,
        label="impacted GID",
    )
