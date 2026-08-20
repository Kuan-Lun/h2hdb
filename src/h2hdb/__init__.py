"""Greenfield public API for the normalized h2hdb schema."""

from __future__ import annotations

__all__ = [
    "CatalogArtifact",
    "CatalogContributor",
    "CatalogIdentifierError",
    "CatalogPage",
    "CatalogPublication",
    "CatalogReader",
    "CatalogRevision",
    "CatalogRevisionNotFoundError",
    "CatalogSubject",
    "ConfigError",
    "CoreConfig",
    "DatabaseAccessMode",
    "DatabaseConfig",
    "DatabaseMaintenanceConfig",
    "EnvironmentPlaceholderError",
    "LoggerConfig",
    "SchemaEpochReadiness",
    "SchemaEpochReport",
    "VNextCatalogFacade",
    "VNextDatabaseAdminFacade",
    "VNextDownloadQueueFacade",
    "load_config",
    "open_database",
    "resolve_environment_placeholders",
]

from .catalog_errors import CatalogIdentifierError, CatalogRevisionNotFoundError
from .config_loader import (
    ConfigError,
    CoreConfig,
    DatabaseAccessMode,
    DatabaseConfig,
    DatabaseMaintenanceConfig,
    LoggerConfig,
    load_config,
)
from .domain import (
    CatalogArtifact,
    CatalogContributor,
    CatalogPage,
    CatalogPublication,
    CatalogRevision,
    CatalogSubject,
)
from .environment import (
    EnvironmentPlaceholderError,
    resolve_environment_placeholders,
)
from .ports import CatalogReader
from .schema_admin import SchemaEpochReadiness
from .schema_epoch import SchemaEpochReport
from .vnext_facade import (
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
    VNextDownloadQueueFacade,
    open_database,
)

__author__ = "Kuan-Lun Wang"
