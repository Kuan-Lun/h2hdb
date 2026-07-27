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

        while connector.insert_h2h_download():
            cycle_start_time = monotonic()
            while connector.insert_h2h_download():
                connector.reset_redownload_times()
                connector.queue_redownload_for_pending_deletions()
                connector.refresh_todelete_names_cache()
                connector.logger.info(
                    "Gallery changes detected; starting another scan immediately."
                )
            connector.logger.info(
                "No gallery changes detected; running database maintenance "
                "before the next scan."
            )
            connector.optimize_database()
            connector.analyze_database()
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
