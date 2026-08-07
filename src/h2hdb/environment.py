__all__ = [
    "EnvironmentPlaceholderError",
    "resolve_environment_placeholders",
]

import os
import re

_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class EnvironmentPlaceholderError(ValueError):
    """Raised when a configuration environment placeholder cannot be resolved."""


def resolve_environment_placeholders(value: object) -> object:
    """Resolve exact ``${ENV_NAME}`` strings recursively without mutating input."""

    if isinstance(value, dict):
        return {
            key: resolve_environment_placeholders(item) for key, item in value.items()
        }
    if isinstance(value, list):
        return [resolve_environment_placeholders(item) for item in value]
    if not isinstance(value, str):
        return value
    if not (value.startswith("${") and value.endswith("}")):
        return value

    name = value[2:-1]
    if _ENVIRONMENT_NAME.fullmatch(name) is None:
        raise EnvironmentPlaceholderError(
            f"Invalid configuration environment variable name {name!r}; "
            "expected [A-Za-z_][A-Za-z0-9_]*"
        )
    try:
        return os.environ[name]
    except KeyError as error:
        raise EnvironmentPlaceholderError(
            f"Configuration environment variable {name!r} is not set"
        ) from error
