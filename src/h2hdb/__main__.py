from time import sleep

from h2hdb import H2HDB

from .config_loader import load_config

SLEEP_INTERVAL_SECONDS = 1800

if __name__ == "__main__":
    config = load_config()
    with H2HDB(config=config) as connector:
        # Check the database character set and collation
        connector.check_database_character_set()
        connector.check_database_collation()
        # Create the main tables
        connector.create_main_tables()

        # Insert the H2H download
        while connector.insert_h2h_download():
            connector.logger.info("Sleeping for 30 minutes...")
            sleep(SLEEP_INTERVAL_SECONDS)
            connector.logger.info("Refreshing database...")

        connector.reset_redownload_times()

        # Reclaim space and refresh optimizer statistics now that the queue is idle
        connector.optimize_database()
        connector.analyze_database()
