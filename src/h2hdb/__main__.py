from __future__ import annotations

import argparse
from collections.abc import Sequence

from .config_loader import load_config
from .logger import setup_logger
from .schema_epoch import SchemaEpochProvider
from .vnext_facade import VNextDatabaseAdminFacade


def main(
    argv: Sequence[str] | None = None,
    *,
    provider: SchemaEpochProvider | None = None,
) -> None:
    parser = argparse.ArgumentParser(description="Administer the H2HDB core schema")
    parser.add_argument(
        "command",
        choices=("migrate", "check", "ready"),
    )
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    logger = setup_logger(config.logger)
    database = VNextDatabaseAdminFacade(config)
    if args.command == "migrate":
        report = database.initialize(provider)
        logger.info(
            "H2HDB schema initialized: "
            f"epoch={report.epoch}, version={report.schema_version}, "
            f"state={report.state}."
        )
    elif args.command == "check":
        report = database.check(provider)
        logger.info(
            "H2HDB schema is valid: "
            f"epoch={report.epoch}, version={report.schema_version}, "
            f"state={report.state}."
        )
    else:
        readiness = database.check_readiness(provider)
        logger.info(
            "H2HDB database is ready: "
            f"epoch={readiness.epoch}, version={readiness.schema_version}, "
            f"manifest={readiness.manifest_sha256}."
        )


if __name__ == "__main__":
    main()
