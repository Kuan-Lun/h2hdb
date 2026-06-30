import os
import zipfile
from collections.abc import Callable
from multiprocessing.pool import Pool
from typing import Any

from h2h_galleryinfo_parser import parse_galleryinfo

from .config_loader import H2HDBConfig
from .repository import BaseRepository, RepositoryContext
from .settings import chunk_list
from .table_times import H2HDBTimes

HASH_LOOKUP_BATCH_SIZE = 500


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
    cbz_path: str, expected_names: frozenset[str]
) -> bool:
    with zipfile.ZipFile(cbz_path) as cbz:
        actual_names = frozenset(cbz.namelist())
    return actual_names != expected_names


def compress_gallery_to_cbz_worker(
    config_data: dict[str, Any],
    gallery_folder: str,
    exclude_hashs: set[bytes],
) -> bool:
    # Deferred to avoid a circular import: h2hdb_h2hdb.py imports this module
    # at module load time, so H2HDB can only be imported lazily, by which
    # point both modules have finished loading.
    from .h2hdb_h2hdb import H2HDB

    config = H2HDBConfig.model_validate(config_data)
    with H2HDB(config=config) as connector:
        return connector.cbz.compress_gallery_to_cbz(gallery_folder, exclude_hashs)


class H2HDBCBZFiles(BaseRepository):
    def __init__(self, context: RepositoryContext, gallery_times: H2HDBTimes) -> None:
        super().__init__(context)
        self.gallery_times = gallery_times

    def _refresh_current_cbz_files(self, current_galleries_names: set[str]) -> None:
        from .compress_gallery_to_cbz import gallery_name_to_cbz_file_name

        current_cbzs: dict[str, str] = dict()
        for root, _, files in os.walk(self.config.h2h.cbz_path):
            for file in files:
                current_cbzs[file] = root
        current_cbz_file_names = {
            gallery_name_to_cbz_file_name(name) for name in current_galleries_names
        }
        for key in set(current_cbzs.keys()) - current_cbz_file_names:
            os.remove(os.path.join(current_cbzs[key], key))
            self.logger.info(f"CBZ '{key}' removed.")
        self.logger.info("CBZ files refreshed.")

        while True:
            directory_removed = False
            for root, dirs, files in os.walk(self.config.h2h.cbz_path, topdown=False):
                if root == self.config.h2h.cbz_path:
                    continue
                if max([len(dirs), len(files)]) == 0:
                    directory_removed = True
                    os.rmdir(root)
                    self.logger.info(f"Directory '{root}' removed.")
            if not directory_removed:
                break
        self.logger.info("Empty directories removed.")

    def compress_gallery_to_cbz(
        self, gallery_folder: str, exclude_hashs: set[bytes]
    ) -> bool:
        from .compress_gallery_to_cbz import (
            compress_images_and_create_cbz,
            expected_output_filename,
            gallery_name_to_cbz_file_name,
        )

        galleryinfo_params = parse_galleryinfo(gallery_folder)
        match self.config.h2h.cbz_grouping:
            case "date-yyyy":
                upload_time = self.gallery_times.get_upload_time_by_gallery_name(
                    galleryinfo_params.gallery_name
                )
                relative_cbz_directory = str(upload_time.year).rjust(4, "0")
            case "date-yyyy-mm":
                upload_time = self.gallery_times.get_upload_time_by_gallery_name(
                    galleryinfo_params.gallery_name
                )
                relative_cbz_directory = os.path.join(
                    str(upload_time.year).rjust(4, "0"),
                    str(upload_time.month).rjust(2, "0"),
                )
            case "date-yyyy-mm-dd":
                upload_time = self.gallery_times.get_upload_time_by_gallery_name(
                    galleryinfo_params.gallery_name
                )
                relative_cbz_directory = os.path.join(
                    str(upload_time.year).rjust(4, "0"),
                    str(upload_time.month).rjust(2, "0"),
                    str(upload_time.day).rjust(2, "0"),
                )
            case "flat":
                relative_cbz_directory = ""
            case _:
                raise ValueError(
                    f"Invalid cbz_grouping value: {self.config.h2h.cbz_grouping}"
                )
        cbz_directory = os.path.join(self.config.h2h.cbz_path, relative_cbz_directory)
        cbz_tmp_directory = os.path.join(self.config.h2h.cbz_path, "tmp")

        cbz_path = os.path.join(
            cbz_directory,
            gallery_name_to_cbz_file_name(galleryinfo_params.gallery_name),
        )

        needs_rebuild = True
        if os.path.exists(cbz_path):
            with self.SQLConnector() as connector:
                rows = connector.fetch_all(
                    "SELECT file_name, sha512 FROM files_hashs WHERE gallery_name = %s",
                    (galleryinfo_params.gallery_name,),
                )
            expected_names = frozenset(
                expected_output_filename(str(file_name))
                for file_name, sha512 in rows
                if bytes(sha512) not in exclude_hashs
            )
            with zipfile.ZipFile(cbz_path) as cbz:
                actual_names = frozenset(cbz.namelist())
            needs_rebuild = actual_names != expected_names

        if needs_rebuild:
            compress_images_and_create_cbz(
                gallery_folder,
                cbz_directory,
                cbz_tmp_directory,
                self.config.h2h.cbz_max_size,
                exclude_hashs,
            )
        return needs_rebuild

    def compress_galleries_to_cbz(
        self, gallery_folders: list[str], exclude_hashs: set[bytes], pool: Pool
    ) -> list[bool]:
        config_data = self.config.model_dump(mode="json")
        return run_in_parallel(
            pool,
            compress_gallery_to_cbz_worker,
            [(config_data, folder, exclude_hashs) for folder in gallery_folders],
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
                    WHERE sha512 IN ({", ".join(["%s"] * len(batch))})
                """
                rows = connector.fetch_all(select_query, tuple(batch))
                affected_galleries.update(str(gallery_name) for (gallery_name,) in rows)
        return affected_galleries

    def _get_files_by_gallery(
        self, gallery_names: set[str]
    ) -> dict[str, list[tuple[str, bytes]]]:
        files_by_gallery: dict[str, list[tuple[str, bytes]]] = dict()
        with self.SQLConnector() as connector:
            for batch in chunk_list(list(gallery_names), HASH_LOOKUP_BATCH_SIZE):
                select_query = f"""
                    SELECT gallery_name, file_name, sha512
                    FROM files_hashs
                    WHERE gallery_name IN ({", ".join(["%s"] * len(batch))})
                """
                rows = connector.fetch_all(select_query, tuple(batch))
                for gallery_name, file_name, sha512 in rows:
                    files_by_gallery.setdefault(str(gallery_name), []).append(
                        (str(file_name), bytes(sha512))
                    )
        return files_by_gallery

    def get_stale_cbz_galleries(
        self, current_galleries_names: set[str], exclude_hashs: set[bytes], pool: Pool
    ) -> set[str]:
        from .compress_gallery_to_cbz import (
            expected_output_filename,
            gallery_name_to_cbz_file_name,
        )

        if not exclude_hashs:
            return set()

        affected_galleries = (
            self._get_galleries_with_excluded_files(exclude_hashs)
            & current_galleries_names
        )
        if not affected_galleries:
            return set()

        files_by_gallery = self._get_files_by_gallery(affected_galleries)

        current_cbzs: dict[str, str] = dict()
        for root, _, files in os.walk(self.config.h2h.cbz_path):
            for file in files:
                current_cbzs[file] = root

        candidates: list[tuple[str, str, frozenset[str]]] = list()
        for gallery_name in affected_galleries:
            gallery_files = files_by_gallery.get(gallery_name, [])
            cbz_file_name = gallery_name_to_cbz_file_name(gallery_name)
            if cbz_file_name not in current_cbzs:
                continue
            cbz_path = os.path.join(current_cbzs[cbz_file_name], cbz_file_name)
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
