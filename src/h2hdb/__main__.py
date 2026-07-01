from time import sleep

from h2hdb import H2HDB

from .config_loader import load_config

SLEEP_INTERVAL_SECONDS = 1800

if __name__ == "__main__":
    config = load_config()
    with H2HDB(config=config) as connector:
        connector.check_database_character_set()
        connector.check_database_collation()
        connector.create_main_tables()

        while connector.insert_h2h_download():
            while connector.insert_h2h_download():
                connector.logger.info("More downloads found, continuing immediately...")
            connector.logger.info("Sleeping for 30 minutes...")
            sleep(SLEEP_INTERVAL_SECONDS)
            connector.logger.info("Checking for new downloads...")

        connector.reset_redownload_times()
        connector.queue_redownload_for_pending_deletions()

        connector.optimize_database()
        connector.analyze_database()
