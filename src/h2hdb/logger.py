__all__ = ["HentaiDBLogger", "setup_logger"]

import logging
from pathlib import Path

from .config_loader import LoggerConfig


def _build_logger(name: str, level: int, handler: logging.Handler) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


class HentaiDBLogger:
    def __init__(self, level: int, file: Path | None = None) -> None:
        screen_handler = logging.StreamHandler()
        screen_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        )
        self.screen_logger = _build_logger("h2hdb.screen", level, screen_handler)
        self.file_logger: logging.Logger | None = None
        if file is not None:
            file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(file, encoding="utf-8")
            file_handler.setFormatter(
                logging.Formatter('"%(asctime)s","%(levelname)s","%(message)s"')
            )
            self.file_logger = _build_logger(
                f"h2hdb.file.{file.resolve()}", level, file_handler
            )

    def debug(self, message: str) -> None:
        self._log(logging.DEBUG, message)

    def info(self, message: str) -> None:
        self._log(logging.INFO, message)

    def warning(self, message: str) -> None:
        self._log(logging.WARNING, message)

    def error(self, message: str) -> None:
        self._log(logging.ERROR, message)

    def critical(self, message: str) -> None:
        self._log(logging.CRITICAL, message)

    def _log(self, level: int, message: str) -> None:
        self.screen_logger.log(level, message)
        if self.file_logger is not None:
            self.file_logger.log(level, message)

    def hasHandlers(self) -> bool:
        return self.screen_logger.hasHandlers() or (
            self.file_logger is not None and self.file_logger.hasHandlers()
        )

    def removeHandlers(self) -> None:
        for logger in (self.screen_logger, self.file_logger):
            if logger is None:
                continue
            for handler in tuple(logger.handlers):
                logger.removeHandler(handler)
                handler.close()

    def addHandler(self, handler: logging.Handler) -> None:
        self.screen_logger.addHandler(handler)


def setup_logger(logger_config: LoggerConfig) -> HentaiDBLogger:
    return HentaiDBLogger(level=logger_config.level, file=logger_config.file)
