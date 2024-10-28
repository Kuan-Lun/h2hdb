__all__ = ["logger"]


from abc import ABCMeta, abstractmethod
import logging
from logging.handlers import MemoryHandler
from functools import partial

from .config_loader import LoggerConfig


LOG_CONFIG = {
    "notset": logging.NOTSET,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.INFO,
}


def setup_screen_logger(level: int) -> logging.Logger:
    screen_logger = logging.getLogger("display_on_screen")
    screen_logger.setLevel(level)

    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    screen_logger.addHandler(handler)
    return screen_logger


def setup_file_logger(level: int) -> logging.Logger:
    log_filename = "h2hdb.log"
    file_logger = logging.getLogger("write_to_file")
    file_logger.setLevel(level)

    with open(log_filename, "w", encoding="utf-8") as f:
        f.write('"time stamp","level","message"\n')

    file_handler = logging.FileHandler(log_filename, mode="a+", encoding="utf-8")
    formatter = logging.Formatter('"%(asctime)s","%(levelname)-8s","%(message)s"')
    file_handler.setFormatter(formatter)

    # MemoryHandler with a capacity of x bytes
    memory_handler = MemoryHandler(
        capacity=1024, target=file_handler, flushLevel=logging.ERROR
    )
    file_logger.addHandler(memory_handler)

    return file_logger


class AbstractLogger(metaclass=ABCMeta):
    @abstractmethod
    def debug(self, message: str) -> None: ...

    @abstractmethod
    def info(self, message: str) -> None: ...

    @abstractmethod
    def warning(self, message: str) -> None: ...

    @abstractmethod
    def error(self, message: str) -> None: ...

    @abstractmethod
    def critical(self, message: str) -> None: ...


class HentaiDBLogger(AbstractLogger):
    def __init__(self, level: str) -> None:
        logging_level = LOG_CONFIG[level.lower()]
        self.screen_logger = setup_screen_logger(logging_level)
        self.file_logger = setup_file_logger(logging_level)

    def debug(self, message: str) -> None:
        self._log_method("debug", message)

    def info(self, message: str) -> None:
        self._log_method("info", message)

    def warning(self, message: str) -> None:
        self._log_method("warning", message)

    def error(self, message: str) -> None:
        self._log_method("error", message)

    def critical(self, message: str) -> None:
        self._log_method("critical", message)

    def _log_method(self, level: str, message: str) -> None:
        log_method_screen = getattr(self.screen_logger, level)
        log_method_file = getattr(self.file_logger, level)
        log_method_screen(message)
        log_method_file(message)

    def hasHandlers(self) -> bool:
        return self.screen_logger.hasHandlers() or self.file_logger.hasHandlers()

    def removeHandlers(self) -> None:
        while self.hasHandlers():
            self.screen_logger.removeHandler(self.screen_logger.handlers[0])
            self.file_logger.removeHandler(self.file_logger.handlers[0])

    def addHandler(self, handler: logging.Handler) -> None:
        self.screen_logger.addHandler(handler)
        self.file_logger.addHandler(handler)


def setup_logger(
    logger_config: LoggerConfig,
) -> HentaiDBLogger:
    return HentaiDBLogger(level=logger_config.level)
