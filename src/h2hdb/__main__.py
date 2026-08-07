import argparse

from .config_loader import load_config
from .service import H2HDB


def main() -> None:
    parser = argparse.ArgumentParser(description="Administer the H2HDB core schema")
    parser.add_argument("command", choices=("migrate", "check"))
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    database = H2HDB(load_config(args.config))
    if args.command == "migrate":
        version = database.migrate()
        database.logger.info(f"H2HDB schema migrated to version {version}.")
    else:
        compatibility = database.check_compatibility()
        database.logger.info(
            "H2HDB schema is compatible: " f"version={compatibility.database_version}."
        )


if __name__ == "__main__":
    main()
