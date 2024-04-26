__all__ = ["logger"]


import logging
import unicodedata
import sys
from functools import partial
import unicodedata
import re
from urllib.parse import urlparse
from urllib.parse import parse_qs
from time import sleep


from .config_loader import load_config, SynoChatConfig

CONFIG_LOADER = load_config()


def parse_uri(uri: str) -> dict:
    parsed_uri = urlparse(uri)

    scheme = parsed_uri.scheme
    authority = parsed_uri.netloc
    path = parsed_uri.path
    query = parse_qs(parsed_uri.query)
    fragment = parsed_uri.fragment

    # 將 query 字典中的值從列表轉換為單一值
    for key, value in query.items():
        if len(value) == 1:
            query[key] = value[0]  # type: ignore

    return {
        "scheme": scheme,
        "authority": authority,
        "path": path,
        "query": query,
        "fragment": fragment,
    }


def split_message_with_cjk(message: str, max_log_entry_length: int) -> list[str]:
    """
    Splits a message into chunks of a specified maximum length, taking into account the width of CJK characters.

    Args:
        message (str): The message to be split.
        max_log_entry_length (int): The maximum length of each chunk. Note that CJK characters count as 2 towards this limit.

    Returns:
        list[str]: A list of chunks, where each chunk is a substring of the original message.
    """
    if max_log_entry_length < 0:
        return [message]
    else:
        chunks = list()
        chunk = str()
        chunk_len = 0
        for char in message:
            char_len = 2 if "CJK UNIFIED" in unicodedata.name(char, str()) else 1
            if chunk_len + char_len > max_log_entry_length:
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
    logger: logging.Logger, level: int, max_log_entry_length: int, message: str
) -> None:
    frame = sys._getframe(3)
    message = re.sub(" +", " ", message.replace("\n", " ")).strip()
    chunks = split_message_with_cjk(message, max_log_entry_length)
    for chunk in chunks:
        record = logger.makeRecord(
            logger.name,
            level,
            frame.f_code.co_filename,
            frame.f_lineno,
            chunk,
            tuple(),
            None,
            frame.f_code.co_name,
        )
        logger.handle(record)


LOG_CONFIG = {
    "notset": logging.NOTSET,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.INFO,
}


def setup_screen_logger(level: int, max_log_entry_length: int) -> logging.Logger:
    """
    Set up a logger that displays log messages on the screen.

    Args:
        level (int): The logging level to set for the logger.
        max_log_entry_length (int): The maximum length of log messages.

    Returns:
        logging.Logger: The configured logger instance.
    """
    screen_logger = logging.getLogger("display_on_screen")
    screen_logger.setLevel(level)

    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    screen_logger.addHandler(handler)

    for level_name, level_value in LOG_CONFIG.items():
        if level_value < level:
            continue
        partial_log_message = partial(
            log_message, screen_logger, level_value, max_log_entry_length
        )
        setattr(screen_logger, level_name, partial_log_message)
    return screen_logger


def setup_file_logger(write_to_file: str, level: int) -> logging.Logger:
    """
    Set up a file logger with the specified log level and log file.

    Args:
        write_to_file (str): The path to the log file.
        level (int): The log level to be set for the file logger.

    Returns:
        logging.Logger: The configured file logger.

    """
    file_logger = logging.getLogger("write_to_file")
    file_logger.setLevel(level)

    with open(write_to_file, "w", encoding="utf-8") as f:
        f.write('"time stamp","level","filename","line no.","message"\n')
    handler = logging.FileHandler(write_to_file, mode="a+", encoding="utf-8")
    formatter = logging.Formatter(
        '"%(asctime)s","%(levelname)-8s","%(filename)-10s","%(lineno)-3d","%(message)s"'
    )
    handler.setFormatter(formatter)
    file_logger.addHandler(handler)

    for level_name, level_value in LOG_CONFIG.items():
        if level_value < level:
            continue
        partial_log_message = partial(log_message, file_logger, level_value, -1)
        setattr(file_logger, level_name, partial_log_message)
    return file_logger


def setup_synochat_webhook_logger(
    webhook_url: SynoChatConfig, level: int
) -> logging.Logger:
    from requests.exceptions import ConnectionError
    from synochat.webhooks import IncomingWebhook  # type: ignore
    from synochat.exceptions import RateLimitError  # type: ignore

    class SynoChatHandler(logging.Handler):
        def __init__(self, synochat_config: SynoChatConfig, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.synochat_config = synochat_config

        def emit(self, record):
            log_entry = self.format(record)
            self.msg2synochat(log_entry, self.synochat_config.webhook_url)

        def msg2synochat(self, s: str, url: str, retry: int = 1) -> None:
            uri = url
            uridict = parse_uri(uri)
            authority = uridict["authority"]
            token = uridict["query"]["token"]
            try:
                webhook = IncomingWebhook(authority, token)
                webhook.send(s)
            except RateLimitError:
                if retry > 0:
                    sleep(60)
                    self.msg2synochat(s, url, retry=retry - 1)
            except ConnectionError:
                print("連不上 Chat")

    webhook_logger = logging.getLogger("webhook_url")
    webhook_logger.setLevel(level)

    handler = SynoChatHandler(webhook_url)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)

    webhook_logger.addHandler(handler)

    for level_name, level_value in LOG_CONFIG.items():
        if level_value < level:
            continue
        partial_log_message = partial(log_message, webhook_logger, level_value, -1)
        setattr(webhook_logger, level_name, partial_log_message)
    return webhook_logger


class HentaiDBLogger:
    """
    The HentaiDBLogger class provides a custom logging interface for the HentaiDB application.

    Attributes:
        level (str): The logging level. Can be one of 'debug', 'info', 'warning', 'error', 'critical'.
        display_on_screen (bool): Whether to display the log messages on screen.
        max_log_entry_length (int): The maximum length of the log messages. This only applies to messages logged to the file specified by `display_on_screen`.
        write_to_file (str): The path to the log file. If an empty string is provided, logging to a file is disabled.

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
        write_to_file: str,
        max_log_entry_length: int,
        synochat_webhook: SynoChatConfig,
    ):
        logging_level = LOG_CONFIG[level.lower()]
        self.display_on_screen = display_on_screen
        if self.display_on_screen:
            self.screen_logger = setup_screen_logger(
                logging_level, max_log_entry_length
            )

        self.write_to_file = write_to_file
        if self.write_to_file != "":
            self.file_logger = setup_file_logger(write_to_file, logging_level)

        if synochat_webhook != "":
            self.synochat_webhook = setup_synochat_webhook_logger(
                synochat_webhook, logging_level
            )

    def hasHandlers(self) -> bool:
        return self.screen_logger.hasHandlers() or self.file_logger.hasHandlers()

    def removeHandlers(self) -> None:
        while self.hasHandlers():
            self.screen_logger.removeHandler(self.screen_logger.handlers[0])
            self.file_logger.removeHandler(self.file_logger.handlers[0])
            self.synochat_webhook.removeHandler(self.synochat_webhook.handlers[0])

    def addHandler(self, handler: logging.Handler) -> None:
        if self.display_on_screen:
            self.screen_logger.addHandler(handler)
        if self.write_to_file != "":
            self.file_logger.addHandler(handler)
        if self.synochat_webhook != "":
            self.synochat_webhook.addHandler(handler)

    def debug(self, message: str) -> None:
        if self.display_on_screen:
            self.screen_logger.debug(message)
        if self.write_to_file != "":
            self.file_logger.debug(message)
        if self.synochat_webhook != "":
            self.synochat_webhook.debug(message)

    def info(self, message: str) -> None:
        if self.display_on_screen:
            self.screen_logger.info(message)
        if self.write_to_file != "":
            self.file_logger.info(message)
        if self.synochat_webhook != "":
            self.synochat_webhook.info(message)

    def warning(self, message: str) -> None:
        if self.display_on_screen:
            self.screen_logger.warning(message)
        if self.write_to_file != "":
            self.file_logger.warning(message)
        if self.synochat_webhook != "":
            self.synochat_webhook.warning(message)

    def error(self, message: str) -> None:
        if self.display_on_screen:
            self.screen_logger.error(message)
        if self.write_to_file != "":
            self.file_logger.error(message)
        if self.synochat_webhook != "":
            self.synochat_webhook.error(message)

    def critical(self, message: str) -> None:
        if self.display_on_screen:
            self.screen_logger.critical(message)
        if self.write_to_file != "":
            self.file_logger.critical(message)
        if self.synochat_webhook != "":
            self.synochat_webhook.critical(message)


def setup_logger(
    level: str,
    display_on_screen: bool,
    write_to_file: str,
    synochat_webhook: SynoChatConfig,
    max_log_entry_length: int = -1,
) -> HentaiDBLogger:
    """
    Set up a logger with the specified logging level and maximum message length.

    This function creates a logger, sets its level to the specified value, and modifies its behavior to split log messages into chunks if they exceed the maximum length.

    Parameters:
    level (str): The logging level as a string. Should be one of "DEBUG", "INFO", "WARNING", "ERROR", or "CRITICAL".
    display_on_screen (bool): Whether to display log messages on the screen.
    write_to_file (str): The path to the log file. If an empty string is provided, logging to a file is disabled.
    max_log_entry_length (int, optional): The maximum length of a log message. If a message exceeds this length, it will be split into multiple chunks. Default is 30.

    Returns:
    logging.Logger: The configured logger.
    """
    return HentaiDBLogger(
        level=level,
        display_on_screen=display_on_screen,
        write_to_file=write_to_file,
        max_log_entry_length=max_log_entry_length,
        synochat_webhook=synochat_webhook,
    )


def reset_logger(logger: HentaiDBLogger) -> None:
    """
    Resets the provided logger by removing all its handlers.

    This function iteratively checks if the logger has any handlers, and if it does, it removes the first handler in the list. This process continues until the logger has no handlers left.

    Args:
        logger (logging.Logger): The logger to be reset.
    """
    logger.removeHandlers()


"""
Set up the logger using the configuration loaded from `CONFIG_LOADER`.
The logging level is set to `CONFIG_LOADER["logger"]["level"]`.
If `CONFIG_LOADER["logger"]["display_on_screen"]` is True, the logger will also output to the console.
If `CONFIG_LOADER["logger"]["write_to_file"]` is not None, the logger will write logs to the specified file.
The maximum length of a log message is set to `CONFIG_LOADER["logger"]["max_log_entry_length"]`. If a message exceeds this length, it will be split into multiple chunks.
"""
logger = setup_logger(
    CONFIG_LOADER.logger.level,
    CONFIG_LOADER.logger.display_on_screen,
    CONFIG_LOADER.logger.write_to_file,
    CONFIG_LOADER.logger.synochat_webhook,
    CONFIG_LOADER.logger.max_log_entry_length,
)


# import os
# import psutil  # type: ignore

# process = psutil.Process(os.getpid())
# displayram = lambda x: logger.info(
#     f"{x}: Memory usage is  {process.memory_info().rss / 1024**2} MB."
# )
