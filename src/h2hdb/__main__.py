from h2hdb import H2HDB
from .config_loader import load_config

if __name__ == "__main__":
    config = load_config()
    with H2HDB(config=config) as connector:
        # Check the database character set and collation
        connector.check_database_character_set()
        connector.check_database_collation()
        # Create the main tables
        connector.create_main_tables()

        # Insert the H2H download
        connector.insert_h2h_download()

        connector.refresh_current_files_hashs()
