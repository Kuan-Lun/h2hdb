# from multiprocessing import Pool
import random
import shutil
from threading import Thread
import os
from functools import partial
from time import sleep

from .threading_tools import add_semaphore_control
from .logger import logger
from h2hdb import H2HDB
from .config_loader import load_config, Config

# from .h2h_db import _insert_h2h_download
from .komga import (
    get_series_ids,
    get_books_ids_in_series_id,
    get_book,
    patch_book_metadata,
    get_series,
    patch_series_metadata,
    scan_library,
    get_books_ids_in_library_id,
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


@add_semaphore_control
def update_komga_book_metadata(config: Config, book_id: str) -> None:
    base_url = config.media_server.server_config.base_url
    api_username = config.media_server.server_config.api_username
    api_password = config.media_server.server_config.api_password
    komga_metadata = get_book(book_id, base_url, api_username, api_password)
    try:
        with H2HDB(config=config) as connector:
            current_metadata = connector.get_komga_metadata(komga_metadata["name"])
        if not (current_metadata.items() <= komga_metadata.items()):
            patch_book_metadata(
                current_metadata, book_id, base_url, api_username, api_password
            )
            logger.debug(f"Book {komga_metadata['name']} updated in the database.")
        else:
            logger.debug(
                f"Book {komga_metadata['name']} already exists in the database."
            )
    except DatabaseKeyError:
        pass


@add_semaphore_control
def update_komga_series_metadata(config: Config, series_id: str) -> None:
    base_url = config.media_server.server_config.base_url
    api_username = config.media_server.server_config.api_username
    api_password = config.media_server.server_config.api_password

    books_ids = get_books_ids_in_series_id(
        series_id, base_url, api_username, api_password
    )

    ischecktitle = False
    for book_id in books_ids:
        komga_metadata = get_book(book_id, base_url, api_username, api_password)
        try:
            with H2HDB(config=config) as connector:
                current_metadata = connector.get_komga_metadata(komga_metadata["name"])
            ischecktitle = True
            break
        except DatabaseKeyError:
            continue

    if ischecktitle:
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
        logger.debug(f"Series_id {series_id} updated in the database.")
    else:
        logger.debug(f"Series_id {series_id} already exists in the database.")


def scan_komga_library(config: Config) -> None:
    library_id = config.media_server.server_config.library_id
    base_url = config.media_server.server_config.base_url
    api_username = config.media_server.server_config.api_username
    api_password = config.media_server.server_config.api_password

    scan_library(library_id, base_url, api_username, api_password)

    # books_ids = get_books_ids_in_all_libraries(base_url, api_username, api_password)
    books_ids = get_books_ids_in_library_id(
        library_id, base_url, api_username, api_password
    )

    threads = list[Thread]()
    for book_id in books_ids:
        thread = Thread(target=update_komga_book_metadata, args=(config, book_id))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    series_ids = get_series_ids(library_id, base_url, api_username, api_password)

    threads = list[Thread]()
    for series_id in series_ids:
        thread = Thread(target=update_komga_series_metadata, args=(config, series_id))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


class UpdateH2HDB:
    def __init__(self, config: Config):
        self.config = config
        if os.path.exists(config.h2h.cbz_tmp_directory):
            shutil.rmtree(config.h2h.cbz_tmp_directory)
        os.makedirs(config.h2h.cbz_tmp_directory)

    def __enter__(self):
        self.thread_running = True
        match self.config.media_server.server_type:
            case "komga":
                self.thread_underlying_target = partial(scan_komga_library, self.config)
            case _:
                self.thread_underlying_target = lambda: None

        def loop_target():
            # n = 0
            while self.thread_running:
                # logger.notset(f"Media server metadata update iteration {n}.")
                self.thread_underlying_target()
                sleep(1)
                # n += 1
            # logger.notset("Media server metadata updated.")

        self.thread = Thread(target=loop_target)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.thread_running = False
        self.thread.join()

    def update_h2hdb(self):
        with H2HDB(config=config) as connector:
            # Check the database character set and collation
            connector.check_database_character_set()
            connector.check_database_collation()
            # Create the main tables
            connector.create_main_tables()

            # Insert the H2H download
            connector.insert_h2h_download()

            connector.refresh_current_files_hashs()


if __name__ == "__main__":
    config = load_config()
    with UpdateH2HDB(config) as update:
        update.update_h2hdb()
