from multiprocessing import Pool
import os
import random

from h2hdb import H2HDB
from .h2h_db import GALLERY_INFO_FILE_NAME
from .config_loader import load_config
from .h2h_db import _insert_h2h_download


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


if __name__ == "__main__":
    config = load_config()
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
            gallery_paths = list()
            for root, _, files in os.walk(connector.config.h2h.download_path):
                if GALLERY_INFO_FILE_NAME in files:
                    gallery_paths.append(root)

    if len(gallery_paths) > 0:
        gallery_groups = random_split_list(
            gallery_paths, config.multiprocess.num_processes
        )
        with Pool(config.multiprocess.num_processes) as pool:
            pool.starmap(
                _insert_h2h_download,
                [(config, gallery_group) for gallery_group in gallery_groups],
            )
