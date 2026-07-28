import datetime
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Any

from h2h_galleryinfo_parser import parse_galleryinfo

from .config_loader import H2HDBConfig
from .gallery_source_manifest import (
    build_cbz_input_manifest,
    cbz_input_manifest_to_comment,
)
from .hash_dict import FILE_CONTENT_HASH_ALGORITHM
from .repository import BaseRepository, RepositoryContext
from .settings import chunk_list, hash_function_by_file
from .table_gids import H2HDBGalleriesIDs
from .table_times import H2HDBTimes

HASH_LOOKUP_BATCH_SIZE = 500


class CBZCompressionOutcome(StrEnum):
    created = "created"
    rebuilt = "rebuilt"
    unchanged = "unchanged"


class ExistingCBZPolicy(StrEnum):
    reconcile = "reconcile"
    preserve = "preserve"


@dataclass
class CBZCompressionSummary:
    checked: int = 0
    created: int = 0
    rebuilt: int = 0
    unchanged: int = 0

    @classmethod
    def from_outcomes(
        cls,
        outcomes: list[CBZCompressionOutcome],
    ) -> CBZCompressionSummary:
        return cls(
            checked=len(outcomes),
            created=outcomes.count(CBZCompressionOutcome.created),
            rebuilt=outcomes.count(CBZCompressionOutcome.rebuilt),
            unchanged=outcomes.count(CBZCompressionOutcome.unchanged),
        )

    @property
    def write_operations(self) -> int:
        return self.created + self.rebuilt

    def __add__(self, other: CBZCompressionSummary) -> CBZCompressionSummary:
        return CBZCompressionSummary(
            checked=self.checked + other.checked,
            created=self.created + other.created,
            rebuilt=self.rebuilt + other.rebuilt,
            unchanged=self.unchanged + other.unchanged,
        )

    def __iadd__(self, other: CBZCompressionSummary) -> CBZCompressionSummary:
        self.checked += other.checked
        self.created += other.created
        self.rebuilt += other.rebuilt
        self.unchanged += other.unchanged
        return self


def run_in_parallel(
    pool: Pool, fun: Callable[..., Any], args: list[tuple[Any, ...]]
) -> list[Any]:
    results: list[Any] = list()
    if args:
        if len(args[0]) > 1:
            results += pool.starmap(fun, args)
        else:
            results += pool.map(fun, [arg[0] for arg in args])
    return results


def cbz_contents_are_stale_worker(
    cbz_path: Path,
    expected_names: frozenset[str],
    expected_input_manifest: bytes | None = None,
) -> bool:
    # A corrupt CBZ (e.g. left behind by a process killed mid-write) is
    # treated as stale so it gets rebuilt, rather than crashing the pool.
    try:
        with zipfile.ZipFile(cbz_path) as cbz:
            actual_names = frozenset(cbz.namelist())
            actual_comment = cbz.comment
    except zipfile.BadZipFile:
        return True
    return actual_names != expected_names or (
        expected_input_manifest is not None
        and actual_comment != cbz_input_manifest_to_comment(expected_input_manifest)
    )


def plan_cbz_compression_worker(
    cbz_path: Path,
    expected_names: frozenset[str],
    expected_input_manifest: bytes,
    existing_cbz_policy: ExistingCBZPolicy,
    force_rebuild: bool,
) -> CBZCompressionOutcome:
    """Classify one CBZ check without writing, including the filesystem stat."""
    if not cbz_path.exists():
        return CBZCompressionOutcome.created
    if existing_cbz_policy == ExistingCBZPolicy.preserve:
        return CBZCompressionOutcome.unchanged
    if force_rebuild:
        return CBZCompressionOutcome.rebuilt
    if cbz_contents_are_stale_worker(
        cbz_path,
        expected_names,
        expected_input_manifest,
    ):
        return CBZCompressionOutcome.rebuilt
    return CBZCompressionOutcome.unchanged


def compress_gallery_to_cbz_worker(
    config_data: dict[str, Any],
    gallery_folder: Path,
    exclude_hashs: set[bytes],
    expected_names: frozenset[str] | None,
    upload_time: datetime.datetime | None,
    input_manifest: bytes,
    force_rebuild: bool,
) -> CBZCompressionOutcome:
    # Deferred to avoid a circular import: h2hdb_h2hdb.py imports this module
    # at module load time, so H2HDB can only be imported lazily, by which
    # point both modules have finished loading.
    from .h2hdb_h2hdb import H2HDB

    config = H2HDBConfig.model_validate(config_data)
    # Not used as a context manager: compress_gallery_to_cbz doesn't touch the
    # database here (expected_names/upload_time are precomputed by the caller),
    # so there's nothing to commit, and H2HDB.__exit__ would otherwise open a
    # connection just to commit nothing.
    connector = H2HDB(config=config)
    return connector.cbz.compress_gallery_to_cbz(
        gallery_folder,
        exclude_hashs,
        expected_names,
        upload_time,
        input_manifest=input_manifest,
        force_rebuild=force_rebuild,
    )


class H2HDBCBZFiles(BaseRepository):
    def __init__(
        self,
        context: RepositoryContext,
        gallery_times: H2HDBTimes,
        gallery_ids: H2HDBGalleriesIDs,
    ) -> None:
        super().__init__(context)
        self.gallery_times = gallery_times
        self.gallery_ids = gallery_ids

    def _get_cbz_output_path(
        self,
        gallery_name: str,
        upload_time: datetime.datetime | None,
    ) -> Path:
        from .compress_gallery_to_cbz import gallery_name_to_cbz_file_name

        assert self.config.h2h.cbz_path is not None
        match self.config.h2h.cbz_grouping:
            case "date-yyyy":
                if upload_time is None:
                    raise ValueError(
                        f"Missing upload time for grouped CBZ: gallery={gallery_name!r}."
                    )
                cbz_directory = self.config.h2h.cbz_path / str(upload_time.year).rjust(
                    4, "0"
                )
            case "date-yyyy-mm":
                if upload_time is None:
                    raise ValueError(
                        f"Missing upload time for grouped CBZ: gallery={gallery_name!r}."
                    )
                cbz_directory = (
                    self.config.h2h.cbz_path
                    / str(upload_time.year).rjust(4, "0")
                    / str(upload_time.month).rjust(2, "0")
                )
            case "date-yyyy-mm-dd":
                if upload_time is None:
                    raise ValueError(
                        f"Missing upload time for grouped CBZ: gallery={gallery_name!r}."
                    )
                cbz_directory = (
                    self.config.h2h.cbz_path
                    / str(upload_time.year).rjust(4, "0")
                    / str(upload_time.month).rjust(2, "0")
                    / str(upload_time.day).rjust(2, "0")
                )
            case "flat":
                cbz_directory = self.config.h2h.cbz_path
            case _:
                raise ValueError(
                    f"Invalid cbz_grouping value: {self.config.h2h.cbz_grouping}"
                )
        return cbz_directory / gallery_name_to_cbz_file_name(gallery_name)

    def _get_cbz_output_paths_by_db_gallery_ids(
        self, db_gallery_ids: list[int]
    ) -> dict[int, Path]:
        gallery_names_by_id = self.gallery_ids.get_gallery_names_by_db_gallery_ids(
            db_gallery_ids
        )
        upload_times_by_id = (
            self.gallery_times.get_upload_times_by_db_gallery_ids(db_gallery_ids)
            if self.config.h2h.cbz_grouping != "flat"
            else {}
        )
        return {
            db_gallery_id: self._get_cbz_output_path(
                gallery_name,
                upload_times_by_id.get(db_gallery_id),
            )
            for db_gallery_id, gallery_name in gallery_names_by_id.items()
        }

    def _get_cbz_output_paths_by_gallery_names(
        self, gallery_names: set[str]
    ) -> dict[str, Path]:
        if self.config.h2h.cbz_grouping == "flat":
            return {
                gallery_name: self._get_cbz_output_path(gallery_name, None)
                for gallery_name in gallery_names
            }

        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                list(gallery_names)
            )
        )
        paths_by_id = self._get_cbz_output_paths_by_db_gallery_ids(
            list(db_gallery_ids_by_name.values())
        )
        paths_by_name = {
            gallery_name: paths_by_id[db_gallery_id]
            for gallery_name, db_gallery_id in db_gallery_ids_by_name.items()
            if db_gallery_id in paths_by_id
        }
        unresolved_names = gallery_names - paths_by_name.keys()
        if unresolved_names:
            raise ValueError(
                "Could not resolve CBZ output paths for current galleries: "
                f"count={len(unresolved_names)}."
            )
        return paths_by_name

    def _delete_cbz_file_for_gallery_name(self, gallery_name: str) -> None:
        from .compress_gallery_to_cbz import gallery_name_to_cbz_file_name

        assert self.config.h2h.cbz_path is not None
        target_file_name = gallery_name_to_cbz_file_name(gallery_name)
        for root, _, files in self.config.h2h.cbz_path.walk():
            if target_file_name in files:
                cbz_path = root / target_file_name
                cbz_path.unlink()
                self.logger.info(
                    "CBZ removed because its gallery record changed: "
                    f"path={str(cbz_path)!r}."
                )
                return

    def _refresh_current_cbz_files(self, current_galleries_names: set[str]) -> None:
        assert self.config.h2h.cbz_path is not None
        current_output_files: list[Path] = []
        for root, _, files in self.config.h2h.cbz_path.walk():
            for file in files:
                current_output_files.append(root / file)
        expected_cbz_paths = set(
            self._get_cbz_output_paths_by_gallery_names(
                current_galleries_names
            ).values()
        )
        removed_files = 0
        for artifact_path in current_output_files:
            if artifact_path in expected_cbz_paths:
                continue
            artifact_path.unlink()
            removed_files += 1
            self.logger.debug(
                "Unexpected CBZ output artifact removed during reconciliation: "
                f"path={str(artifact_path)!r}."
            )

        removed_directories = 0
        while True:
            directory_removed = False
            for root, dirs, files in self.config.h2h.cbz_path.walk(top_down=False):
                if root == self.config.h2h.cbz_path:
                    continue
                if max([len(dirs), len(files)]) == 0:
                    directory_removed = True
                    root.rmdir()
                    removed_directories += 1
                    self.logger.debug(
                        f"Empty CBZ output directory removed: path={str(root)!r}."
                    )
            if not directory_removed:
                break
        self.logger.info(
            "CBZ output reconciliation completed: "
            f"scanned_files={len(current_output_files)} "
            f"expected_cbz_files={len(expected_cbz_paths)} "
            f"unexpected_files_removed={removed_files} "
            f"empty_directories_removed={removed_directories}."
        )

    def compress_gallery_to_cbz(
        self,
        gallery_folder: Path,
        exclude_hashs: set[bytes],
        expected_names: frozenset[str] | None,
        upload_time: datetime.datetime | None,
        *,
        input_manifest: bytes | None = None,
        force_rebuild: bool = False,
    ) -> CBZCompressionOutcome:
        from .compress_gallery_to_cbz import compress_images_and_create_cbz

        assert self.config.h2h.cbz_path is not None
        galleryinfo_params = parse_galleryinfo(gallery_folder)
        if input_manifest is None:
            input_manifest = build_cbz_input_manifest(
                (
                    file_path.name,
                    hash_function_by_file(
                        file_path,
                        FILE_CONTENT_HASH_ALGORITHM,
                    ),
                )
                for file_path in galleryinfo_params.files_path
            )
        cbz_path = self._get_cbz_output_path(
            galleryinfo_params.gallery_name, upload_time
        )
        cbz_directory = cbz_path.parent
        cbz_tmp_directory = self.config.h2h.cbz_path / "tmp"

        cbz_existed = cbz_path.exists()
        needs_rebuild = force_rebuild or not cbz_existed or expected_names is None
        if cbz_existed and not force_rebuild and expected_names is not None:
            # A corrupt CBZ (e.g. left behind by a process killed mid-write)
            # is treated as needing a rebuild, rather than raising here.
            try:
                with zipfile.ZipFile(cbz_path) as cbz:
                    actual_names = frozenset(cbz.namelist())
                    actual_comment = cbz.comment
                needs_rebuild = (
                    actual_names != expected_names
                    or actual_comment != cbz_input_manifest_to_comment(input_manifest)
                )
            except zipfile.BadZipFile:
                needs_rebuild = True

        if needs_rebuild:
            compress_images_and_create_cbz(
                gallery_folder,
                cbz_directory,
                cbz_tmp_directory,
                self.config.h2h.cbz_max_size,
                exclude_hashs,
                input_manifest,
                self.logger.warning,
            )
            return (
                CBZCompressionOutcome.rebuilt
                if cbz_existed
                else CBZCompressionOutcome.created
            )
        return CBZCompressionOutcome.unchanged

    def compress_galleries_to_cbz(
        self,
        gallery_folders: list[Path],
        exclude_hashs: set[bytes],
        pool: Pool,
        *,
        existing_cbz_policy: ExistingCBZPolicy = ExistingCBZPolicy.reconcile,
        force_rebuild_gallery_names: set[str] | None = None,
    ) -> CBZCompressionSummary:
        if not gallery_folders:
            return CBZCompressionSummary()

        from .compress_gallery_to_cbz import expected_output_filename

        force_rebuild_gallery_names = force_rebuild_gallery_names or set()
        unknown_force_rebuild_names = force_rebuild_gallery_names.difference(
            gallery_folder.name for gallery_folder in gallery_folders
        )
        if unknown_force_rebuild_names:
            raise ValueError(
                "Forced CBZ rebuild names are not in gallery_folders: "
                f"{sorted(unknown_force_rebuild_names)}"
            )
        if (
            existing_cbz_policy == ExistingCBZPolicy.preserve
            and force_rebuild_gallery_names
        ):
            raise ValueError(
                "Cannot force CBZ rebuilds while preserving existing CBZ files."
            )

        # Precompute everything compress_gallery_to_cbz needs from the database
        # here, in batched queries keyed on db_gallery_id, and pass the results
        # into each worker -- instead of every worker process opening its own
        # connection to look the same things up one gallery at a time via
        # gallery_name. galleries_names.full_name only has a FULLTEXT index
        # (confirmed with EXPLAIN), so an equality/IN lookup on it can't use an
        # index and scans close to the whole files/galleries tables regardless
        # of batch size; galleries_dbids' split name columns are a real UNIQUE
        # index, so resolving names to db_gallery_id there first keeps every
        # subsequent lookup an indexed one.
        gallery_names = {folder.name for folder in gallery_folders}
        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                list(gallery_names)
            )
        )
        files_by_db_gallery_id = self._get_files_by_db_gallery_ids(
            list(db_gallery_ids_by_name.values())
        )
        expected_names_by_gallery = {
            gallery_name: frozenset(
                expected_output_filename(file_name)
                for file_name, file_hash in files_by_db_gallery_id.get(
                    db_gallery_id, []
                )
                if file_hash not in exclude_hashs
            )
            for gallery_name, db_gallery_id in db_gallery_ids_by_name.items()
        }
        input_manifests_by_gallery = {
            gallery_name: build_cbz_input_manifest(
                files_by_db_gallery_id.get(db_gallery_id, [])
            )
            for gallery_name, db_gallery_id in db_gallery_ids_by_name.items()
        }

        upload_time_by_gallery: dict[str, datetime.datetime] = {}
        if self.config.h2h.cbz_grouping != "flat":
            upload_times_by_id = self.gallery_times.get_upload_times_by_db_gallery_ids(
                list(db_gallery_ids_by_name.values())
            )
            upload_time_by_gallery = {
                gallery_name: upload_times_by_id[db_gallery_id]
                for gallery_name, db_gallery_id in db_gallery_ids_by_name.items()
                if db_gallery_id in upload_times_by_id
            }

        expected_names = [
            expected_names_by_gallery[gallery_folder.name]
            for gallery_folder in gallery_folders
        ]
        input_manifests = [
            input_manifests_by_gallery[gallery_folder.name]
            for gallery_folder in gallery_folders
        ]
        upload_times = [
            upload_time_by_gallery.get(gallery_folder.name)
            for gallery_folder in gallery_folders
        ]
        cbz_paths = [
            self._get_cbz_output_path(gallery_folder.name, upload_time)
            for gallery_folder, upload_time in zip(
                gallery_folders, upload_times, strict=True
            )
        ]
        planned_outcomes = run_in_parallel(
            pool,
            plan_cbz_compression_worker,
            [
                (
                    cbz_path,
                    gallery_expected_names,
                    input_manifest,
                    existing_cbz_policy,
                    gallery_folder.name in force_rebuild_gallery_names,
                )
                for (
                    gallery_folder,
                    cbz_path,
                    gallery_expected_names,
                    input_manifest,
                ) in zip(
                    gallery_folders,
                    cbz_paths,
                    expected_names,
                    input_manifests,
                    strict=True,
                )
            ],
        )
        typed_outcomes = [
            CBZCompressionOutcome(outcome) for outcome in planned_outcomes
        ]

        planned_write_indices = [
            index
            for index, outcome in enumerate(typed_outcomes)
            if outcome != CBZCompressionOutcome.unchanged
        ]

        config_data = self.config.model_dump(mode="json")
        written_outcomes = run_in_parallel(
            pool,
            compress_gallery_to_cbz_worker,
            [
                (
                    config_data,
                    gallery_folders[index],
                    exclude_hashs,
                    expected_names[index],
                    upload_times[index],
                    input_manifests[index],
                    True,
                )
                for index in planned_write_indices
            ],
        )
        for index, outcome in zip(planned_write_indices, written_outcomes, strict=True):
            typed_outcomes[index] = CBZCompressionOutcome(outcome)

        for gallery_folder, outcome in zip(
            gallery_folders, typed_outcomes, strict=True
        ):
            self.logger.debug(
                "CBZ compression result: "
                f"gallery={gallery_folder.name!r} outcome={outcome.value}."
            )
        return CBZCompressionSummary.from_outcomes(
            typed_outcomes,
        )

    def _get_galleries_with_excluded_files(self, exclude_hashs: set[bytes]) -> set[str]:
        # Filters by exclude_hashs (typically a handful of duplicate/spam
        # hashes) rather than by gallery name (roughly the whole collection),
        # so both the row count and the number of batched round trips stay
        # small instead of scaling with the total number of galleries.
        affected_galleries = set[str]()
        with self.SQLConnector() as connector:
            for batch in chunk_list(list(exclude_hashs), HASH_LOOKUP_BATCH_SIZE):
                select_query = f"""
                    SELECT DISTINCT gallery_name
                    FROM files_hashs
                    WHERE {FILE_CONTENT_HASH_ALGORITHM} IN ({", ".join(["%s"] * len(batch))})
                """
                rows = connector.fetch_all(select_query, tuple(batch))
                affected_galleries.update(str(gallery_name) for (gallery_name,) in rows)
        return affected_galleries

    def _get_files_by_db_gallery_ids(
        self, db_gallery_ids: list[int]
    ) -> dict[int, list[tuple[str, bytes]]]:
        # Joins from files_dbids (filtered on the indexed db_gallery_id) rather
        # than through the files_hashs view's gallery_name column, which only
        # has a FULLTEXT index and can't serve an equality/IN lookup.
        files_by_db_gallery_id: dict[int, list[tuple[str, bytes]]] = dict()
        hash_table_name = f"files_hashs_{FILE_CONTENT_HASH_ALGORITHM}"
        hash_dbids_table_name = f"{hash_table_name}_dbids"
        with self.SQLConnector() as connector:
            for batch in chunk_list(db_gallery_ids, HASH_LOOKUP_BATCH_SIZE):
                select_query = f"""
                    SELECT files_dbids.db_gallery_id,
                        files_names.full_name,
                        {hash_dbids_table_name}.hash_value
                    FROM files_dbids
                        JOIN files_names
                            ON files_names.db_file_id = files_dbids.db_file_id
                        LEFT JOIN {hash_table_name}
                            ON {hash_table_name}.db_file_id = files_dbids.db_file_id
                        LEFT JOIN {hash_dbids_table_name}
                            ON {hash_dbids_table_name}.db_hash_id = {hash_table_name}.db_hash_id
                    WHERE files_dbids.db_gallery_id IN ({", ".join(["%s"] * len(batch))})
                """
                rows = connector.fetch_all(select_query, tuple(batch))
                for db_gallery_id, file_name, content_hash in rows:
                    files_by_db_gallery_id.setdefault(int(db_gallery_id), []).append(
                        (str(file_name), bytes(content_hash))
                    )
        return files_by_db_gallery_id

    def get_stale_cbz_galleries(
        self, current_galleries_names: set[str], exclude_hashs: set[bytes], pool: Pool
    ) -> set[str]:
        from .compress_gallery_to_cbz import expected_output_filename

        assert self.config.h2h.cbz_path is not None
        if not exclude_hashs:
            return set()

        affected_galleries = (
            self._get_galleries_with_excluded_files(exclude_hashs)
            & current_galleries_names
        )
        if not affected_galleries:
            return set()

        db_gallery_ids_by_name = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                list(affected_galleries)
            )
        )
        files_by_db_gallery_id = self._get_files_by_db_gallery_ids(
            list(db_gallery_ids_by_name.values())
        )

        cbz_paths_by_id = self._get_cbz_output_paths_by_db_gallery_ids(
            list(db_gallery_ids_by_name.values())
        )

        candidates: list[tuple[str, Path, frozenset[str]]] = list()
        for gallery_name in affected_galleries:
            db_gallery_id = db_gallery_ids_by_name.get(gallery_name)
            gallery_files = (
                files_by_db_gallery_id.get(db_gallery_id, [])
                if db_gallery_id is not None
                else []
            )
            cbz_path = (
                cbz_paths_by_id.get(db_gallery_id)
                if db_gallery_id is not None
                else None
            )
            if cbz_path is None or not cbz_path.exists():
                continue
            expected_names = frozenset(
                expected_output_filename(file_name)
                for file_name, file_hash in gallery_files
                if file_hash not in exclude_hashs
            )
            candidates.append((gallery_name, cbz_path, expected_names))

        if not candidates:
            return set()

        is_stale_list = run_in_parallel(
            pool,
            cbz_contents_are_stale_worker,
            [(cbz_path, expected_names) for _, cbz_path, expected_names in candidates],
        )
        return {
            gallery_name
            for (gallery_name, _, _), is_stale in zip(
                candidates, is_stale_list, strict=True
            )
            if is_stale
        }
