"""Logging settings retained by the greenfield configuration surface."""

from __future__ import annotations

__all__ = ["LOG_LEVEL"]

import logging
from enum import Enum


class LOG_LEVEL(int, Enum):
    notset = logging.NOTSET
    debug = logging.DEBUG
    info = logging.INFO
    warning = logging.WARNING
    error = logging.ERROR
    critical = logging.CRITICAL
