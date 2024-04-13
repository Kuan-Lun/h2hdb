from multiprocessing import Pool
import random
import shutil
import os

from .logger import logger
from h2hdb import H2HDB
from .h2h_db import GALLERY_INFO_FILE_NAME
from .config_loader import load_config, Config
from .h2h_db import _insert_h2h_download
from .komga import (
    get_series_ids,
    get_books_ids_in_series_id,
    get_book,
    patch_book_metadata,
    get_series,
    patch_series_metadata,
    download_book,
    scan_library,
    get_books_ids_in_all_libraries,
)
from .sql_connector import DatabaseKeyError


def random_split_list(input_list: list, num_groups: int) -> list[list]:
    # Randomly shuffle the input list
    random.shuffle(input_list)

    # Calculate the size of each group
    group_size = len(input_list) // num_groups

    # Create the groups
    if group_size == 0:
        groups = [input_list]
    else:
        groups = [
            input_list[i : i + group_size]
            for i in range(0, len(input_list), group_size)
        ]

        # Distribute the remainder
        remainder = len(input_list) % num_groups
        for i in range(remainder):
            groups[i].append(input_list[-(i + 1)])

    return groups


def count_directories(path: str) -> int:
    return len(
        [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
    )


def scan_komga_library(config: Config) -> None:
    library_id = config.media_server.server_config.library_id
    base_url = config.media_server.server_config.base_url
    api_username = config.media_server.server_config.api_username
    api_password = config.media_server.server_config.api_password

    scan_library(library_id, base_url, api_username, api_password)

    books_ids = get_books_ids_in_all_libraries(base_url, api_username, api_password)
    with H2HDB(config=config) as connector:
        for n, book_id in enumerate(books_ids):
            logger.info(
                f"Scanning book {(n+1)}/{len(books_ids)}={(n+1)/len(books_ids):.2%} ({book_id})"
            )
            komga_metadata = get_book(book_id, base_url, api_username, api_password)
            try:
                current_metadata = connector.get_komga_metadata(komga_metadata["name"])
            except DatabaseKeyError:
                # import io
                # import os
                # import zipfile

                # logger.info(f"Download book {komga_metadata['name']}")
                # cbz_io = io.BytesIO(
                #     download_book(book_id, base_url, api_username, api_password)
                # )
                # download_dir_num = 0
                # while True:
                #     downloadpath = os.path.join(
                #         ".",
                #         ".tmp",
                #         "download",
                #         "{n:03d}".format(n=download_dir_num),
                #     )
                #     if not os.path.exists(downloadpath):
                #         os.makedirs(downloadpath)
                #         break
                #     if count_directories(downloadpath) < 300:
                #         break
                #     download_dir_num += 1
                # try:
                #     with zipfile.ZipFile(cbz_io, "r") as zip_ref:
                #         # 檢查每一個檔案是否為 'abc.txt'
                #         for filename in zip_ref.namelist():
                #             if filename == GALLERY_INFO_FILE_NAME:
                #                 # 如果是，則讀取並寫入到檔案
                #                 os.makedirs(
                #                     os.path.join(downloadpath, komga_metadata["name"])
                #                 )
                #                 with open(
                #                     os.path.join(
                #                         downloadpath, komga_metadata["name"], filename
                #                     ),
                #                     "wb",
                #                 ) as f:
                #                     f.write(zip_ref.read(filename))
                # except FileExistsError:
                #     pass
                continue

            if not (current_metadata.items() <= komga_metadata.items()):
                patch_book_metadata(
                    current_metadata, book_id, base_url, api_username, api_password
                )

    series_ids = get_series_ids(library_id, base_url, api_username, api_password)
    with H2HDB(config=config) as connector:
        for n, series_id in enumerate(series_ids):
            logger.info(
                f"Scanning series {(n+1)}/{len(series_ids)}={(n+1)/len(series_ids):.2%} ({series_id})"
            )
            books_ids = get_books_ids_in_series_id(
                series_id, base_url, api_username, api_password
            )

            ischecktitle = False
            for book_id in books_ids:
                komga_metadata = get_book(book_id, base_url, api_username, api_password)
                try:
                    current_metadata = connector.get_komga_metadata(
                        komga_metadata["name"]
                    )
                    ischecktitle = True
                    break
                except DatabaseKeyError:
                    continue

            if not ischecktitle:
                continue
            series_title = get_series(series_id, base_url, api_username, api_password)[
                "metadata"
            ]["title"]
            if series_title != current_metadata["releaseDate"]:
                patch_series_metadata(
                    {"title": current_metadata["releaseDate"]},
                    series_id,
                    base_url,
                    api_username,
                    api_password,
                )


if __name__ == "__main__":
    config = load_config()
    while True:
        if os.path.exists(config.h2h.cbz_tmp_directory):
            shutil.rmtree(config.h2h.cbz_tmp_directory)
        os.makedirs(config.h2h.cbz_tmp_directory)
        with H2HDB(config=config) as connector:
            # Check the database character set and collation
            connector.check_database_character_set()
            connector.check_database_collation()
            # Create the main tables
            connector.create_main_tables()

            # Insert the H2H download
            if config.multiprocess.num_processes == 1:
                connector.insert_h2h_download()
            else:
                connector.delete_pending_gallery_removals()
                current_galleries_folders = connector.scan_current_galleries_folders()

        if config.multiprocess.num_processes > 1 and len(current_galleries_folders) > 0:
            gallery_groups = random_split_list(
                current_galleries_folders, config.multiprocess.num_processes
            )
            with Pool(config.multiprocess.num_processes) as pool:
                pool.starmap(
                    _insert_h2h_download,
                    [(config, gallery_group) for gallery_group in gallery_groups],
                )

        with H2HDB(config=config) as connector:
            connector.refresh_current_files_hashs()

        scan_komga_library(config)
