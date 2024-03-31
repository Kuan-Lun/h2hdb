__all__ = ["logger"]


import logging
import unicodedata
import sys
from types import FrameType

from .config_loader import config_loader


def is_cjk(char: str) -> bool:
    return "CJK UNIFIED" in unicodedata.name(char, str())


def str_len(s: str) -> int:
    length = 0
    for char in s:
        if is_cjk(char):
            length += 2
        else:
            length += 1
    return length


def split_message(message: str, max_length: int) -> list[str]:
    chunks = list()
    chunk = str()
    chunk_len = 0
    for char in message:
        char_len = 2 if is_cjk(char) else 1
        if chunk_len + char_len > max_length:
            if chunk and chunk[0] == " ":
                chunk = chunk[1:]
            chunks.append(chunk)
            chunk = str()
            chunk_len = 0
        chunk += char
        chunk_len += char_len
    if chunk:  # don't forget to append the last chunk
        if chunk[0] == " ":
            chunk = chunk[1:]
        chunks.append(chunk)
    return chunks


def log_message(
    logger: logging.Logger, level: int, message: str, max_length: int, frame: FrameType
) -> None:
    chunks = split_message(message, max_length)
    for chunk in chunks:
        record = logger.makeRecord(
            logger.name,
            level,
            frame.f_code.co_filename,
            frame.f_lineno,
            chunk,
            None,
            None,
            frame.f_code.co_name,
        )
        logger.handle(record)


def reset_level(level: str) -> int:
    """
    Convert a string representation of a logging level to its corresponding constant.

    Parameters:
    level (str): The logging level as a string. Should be one of "DEBUG", "INFO", "WARNING", "ERROR", or "CRITICAL".

    Returns:
    int: The logging level as a constant. If the input is not a valid logging level, a ValueError is raised.
    """
    LOG_LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    level = level.upper()
    if level not in LOG_LEVELS:
        raise ValueError(f"Invalid logging level: {level}")
    return LOG_LEVELS[level]


class HentaiDBLogger:
    """
    The HentaiDBLogger class provides a custom logging interface for the HentaiDB application.

    Attributes:
        level (str): The logging level. Can be one of 'debug', 'info', 'warning', 'error', 'critical'.
        display_on_screen (bool): Whether to display the log messages on screen.
        display_on_file (str): The path of the log file to which the log messages are written.
        max_length (int): The maximum length of the log messages. This only applies to messages logged to the file specified by `display_on_file`.

    It allows logging messages at different levels (debug, info, warning, error, critical) to both the screen and a log file.
    The logging level, whether to display on screen, the log file path, and the maximum log length can be configured upon initialization.

    Methods:
        hasHandlers() -> bool:
            Checks if any handlers are attached to the logger.

        removeHandlers() -> None:
            Removes all handlers attached to the logger.

        addHandler(handler: logging.Handler) -> None:
            Adds a new handler to the logger.

        debug(message: str) -> None:
            Logs a debug message.

        info(message: str) -> None:
            Logs an info message.

        warning(message: str) -> None:
            Logs a warning message.

        error(message: str) -> None:
            Logs an error message.

        critical(message: str) -> None:
            Logs a critical message.
    """

    def __init__(
        self,
        level: str,
        display_on_screen: bool,
        display_on_file: str,
        max_length: int,
    ):
        logging_level = reset_level(level)

        def setup_screen_logger() -> logging.Logger:
            screen_logger = logging.getLogger("display_on_screen")
            screen_logger.setLevel(logging_level)

            if display_on_screen:
                handler = logging.StreamHandler()
                formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
                handler.setFormatter(formatter)
                screen_logger.addHandler(handler)
            return screen_logger

        self.screen_logger = setup_screen_logger()

        def setup_file_logger() -> logging.Logger:
            file_logger = logging.getLogger("display_on_file")
            file_logger.setLevel(logging_level)
            if display_on_file is not None:
                with open(display_on_file, "w", encoding="utf-8") as f:
                    f.write(
                        '"time stamp","name","level","filename","function","line no.","message"\n'
                    )
                handler = logging.FileHandler(
                    display_on_file, mode="a+", encoding="utf-8"
                )
                formatter = logging.Formatter(
                    '"%(asctime)s","%(name)-6s","%(levelname)-8s","%(filename)-10s","%(funcName)-10s","%(lineno)-3d","%(message)s"'
                )
                handler.setFormatter(formatter)
                file_logger.addHandler(handler)

            log_config = {
                "debug": logging.DEBUG,
                "info": logging.INFO,
                "warning": logging.WARNING,
                "error": logging.ERROR,
                "critical": logging.INFO,
            }

            for level_name, level in log_config.items():
                setattr(
                    file_logger,
                    level_name,
                    lambda message, level=level: log_message(
                        logger=file_logger,
                        level=level,
                        message=message,
                        max_length=max_length,
                        frame=sys._getframe(2),
                    ),
                )
            return file_logger

        self.file_logger = setup_file_logger()

    def hasHandlers(self) -> bool:
        return self.screen_logger.hasHandlers() or self.file_logger.hasHandlers()

    def removeHandlers(self) -> None:
        while self.hasHandlers():
            self.screen_logger.removeHandler(self.screen_logger.handlers[0])
            self.file_logger.removeHandler(self.file_logger.handlers[0])

    def addHandler(self, handler: logging.Handler) -> None:
        self.screen_logger.addHandler(handler)
        self.file_logger.addHandler(handler)

    def debug(self, message: str) -> None:
        self.screen_logger.debug(message)
        self.file_logger.debug(message)

    def info(self, message: str) -> None:
        self.screen_logger.info(message)
        self.file_logger.info(message)

    def warning(self, message: str) -> None:
        self.screen_logger.warning(message)
        self.file_logger.warning(message)

    def error(self, message: str) -> None:
        self.screen_logger.error(message)
        self.file_logger.error(message)

    def critical(self, message: str) -> None:
        self.screen_logger.critical(message)
        self.file_logger.critical(message)


def setup_logger(
    level: str,
    display_on_screen: bool = False,
    display_on_file: str = None,
    max_length: int = 50,
) -> HentaiDBLogger:
    """
    Set up a logger with the specified logging level and maximum message length.

    This function creates a logger, sets its level to the specified value, and modifies its behavior to split log messages into chunks if they exceed the maximum length.

    Parameters:
    level (str): The logging level as a string. Should be one of "DEBUG", "INFO", "WARNING", "ERROR", or "CRITICAL".
    max_length (int, optional): The maximum length of a log message. If a message exceeds this length, it will be split into multiple chunks. Default is 30.

    Returns:
    logging.Logger: The configured logger.
    """
    return HentaiDBLogger(level, display_on_screen, display_on_file, max_length)


def reset_logger(logger: HentaiDBLogger) -> None:
    """
    Resets the provided logger by removing all its handlers.

    This function iteratively checks if the logger has any handlers, and if it does, it removes the first handler in the list. This process continues until the logger has no handlers left.

    Args:
        logger (logging.Logger): The logger to be reset.
    """
    logger.removeHandlers()


"""
Set up the logger using the configuration loaded from `config_loader`.
The logging level is set to `config_loader["logger"]["level"]`.
If `config_loader["logger"]["display_on_screen"]` is True, the logger will also output to the console.
If `config_loader["logger"]["display_on_file"]` is not None, the logger will write logs to the specified file.
The maximum length of a log message is set to `config_loader["logger"]["max_length"]`. If a message exceeds this length, it will be split into multiple chunks.
"""
logger = setup_logger(
    config_loader["logger"]["level"],
    config_loader["logger"]["display_on_screen"],
    config_loader["logger"]["display_on_file"],
    config_loader["logger"]["max_length"],
)
