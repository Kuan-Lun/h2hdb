from time import monotonic, sleep

from h2hdb import H2HDB

from .config_loader import ensure_download_path_ready, load_config

SLEEP_INTERVAL_SECONDS = 1800

if __name__ == "__main__":
    config = load_config()
    ensure_download_path_ready(config.h2h.download_path)
    with H2HDB(config=config) as connector:
        connector.check_database_character_set()
        connector.check_database_collation()
        connector.create_main_tables()

        while True:
            cycle_start_time = monotonic()
            while True:
                outcome = connector.synchronize_once()
                if not outcome.needs_immediate_rescan:
                    break
                connector.reset_redownload_times()
                connector.logger.info(
                    "Gallery insertions or metadata changes detected; "
                    "starting another scan immediately."
                )
            connector.run_scheduled_database_maintenance()
            remaining_sleep_seconds = SLEEP_INTERVAL_SECONDS - (
                monotonic() - cycle_start_time
            )
            if remaining_sleep_seconds > 0:
                connector.logger.info(
                    f"Sleeping for {remaining_sleep_seconds:.0f} seconds..."
                )
                sleep(remaining_sleep_seconds)
            else:
                connector.logger.info(
                    "Cycle already took longer than the sleep interval; skipping sleep."
                )
