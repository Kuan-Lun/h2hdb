from __future__ import annotations

import argparse
from collections.abc import Sequence
from contextlib import closing

from .config_loader import load_config
from .logger import _route_database_diagnostics, setup_logger
from .vnext_facade import VNextDatabaseAdminFacade


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Administer the H2HDB core schema")
    parser.add_argument(
        "command",
        choices=("migrate", "check", "ready"),
    )
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    logger = setup_logger(config.logger)
    with (
        _route_database_diagnostics(logger, level=int(config.logger.level)),
        closing(VNextDatabaseAdminFacade(config)) as database,
    ):
        match args.command:
            case "migrate":
                provisioned = database.initialize()
                audit = (
                    "activation_full"
                    if provisioned.activation_audit is not None
                    else "not_performed; use check for a full READY audit"
                )
                logger.info(
                    "H2HDB schema provisioned: "
                    f"outcome={provisioned.outcome.value}, epoch={provisioned.epoch}, "
                    f"version={provisioned.schema_version}, state={provisioned.state}, "
                    f"audit={audit}."
                )
            case "check":
                report = database.check()
                logger.info(
                    "H2HDB schema is valid: "
                    f"epoch={report.epoch}, version={report.schema_version}, "
                    f"state={report.state}."
                )
            case _:
                readiness = database.check_readiness()
                logger.info(
                    "H2HDB database is ready: "
                    f"epoch={readiness.epoch}, version={readiness.schema_version}, "
                    f"manifest={readiness.manifest_sha256}."
                )


if __name__ == "__main__":
    main()
