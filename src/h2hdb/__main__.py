import argparse
from collections.abc import Sequence

from .config_loader import load_config
from .service import H2HDB


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Administer the H2HDB core schema")
    parser.add_argument(
        "command",
        choices=(
            "migrate",
            "check",
            "ready",
            "epoch-v2-initialize",
            "epoch-v2-check",
            "epoch-v2-ready",
        ),
    )
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    database = H2HDB(load_config(args.config))
    if args.command == "migrate":
        version = database.migrate()
        database.logger.info(f"H2HDB schema migrated to version {version}.")
    elif args.command == "check":
        compatibility = database.check_compatibility()
        database.logger.info(
            "H2HDB schema is compatible: " f"version={compatibility.database_version}."
        )
    elif args.command == "ready":
        compatibility = database.check_readiness()
        database.logger.info(
            "H2HDB database is ready: " f"version={compatibility.database_version}."
        )
    elif args.command == "epoch-v2-initialize":
        report = database.initialize_schema_epoch_v2()
        database.logger.info(
            "H2HDB schema epoch v2 initialized: "
            f"epoch={report.epoch}, version={report.schema_version}, "
            f"state={report.state}."
        )
    elif args.command == "epoch-v2-check":
        report = database.check_schema_epoch_v2()
        database.logger.info(
            "H2HDB schema epoch v2 is valid: "
            f"epoch={report.epoch}, version={report.schema_version}, "
            f"state={report.state}."
        )
    else:
        readiness = database.check_schema_epoch_v2_readiness()
        database.logger.info(
            "H2HDB schema epoch v2 is ready: "
            f"epoch={readiness.epoch}, version={readiness.schema_version}, "
            f"manifest={readiness.manifest_sha256}."
        )


if __name__ == "__main__":
    main()
