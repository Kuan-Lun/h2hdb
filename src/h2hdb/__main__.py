import shutil
from threading import Thread
import os
from functools import partial
from time import sleep

from h2hdb import H2HDB
from .config_loader import load_config, Config
from .komga import scan_komga_library


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
            while self.thread_running:
                self.thread_underlying_target()
                sleep(1)

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
