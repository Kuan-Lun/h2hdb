import logging
import unicodedata

__all__ = ["logger"]


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
    level: int, message: str, max_length: int, original_log_message: callable
) -> None:
    chunks = split_message(message, max_length)
    for chunk in chunks:
        match level:
            case logging.DEBUG:
                original_log_message(chunk)
            case logging.INFO:
                original_log_message(chunk)
            case logging.WARNING:
                original_log_message(chunk)
            case logging.ERROR:
                original_log_message(chunk)
            case logging.CRITICAL:
                original_log_message(chunk)


def setup_logger(level: str, max_length: int = 30) -> logging.Logger:
    """
    Set up a logger with the specified logging level and maximum message length.

    This function creates a logger, sets its level to the specified value, and modifies its behavior to split log messages into chunks if they exceed the maximum length.

    Parameters:
    level (str): The logging level as a string. Should be one of "DEBUG", "INFO", "WARNING", "ERROR", or "CRITICAL".
    max_length (int, optional): The maximum length of a log message. If a message exceeds this length, it will be split into multiple chunks. Default is 30.

    Returns:
    logging.Logger: The configured logger.
    """

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

    logger = logging.getLogger()
    logging_level = reset_level(level)
    logger.setLevel(logging_level)
    formatter = logging.Formatter(
        '"%(asctime)s","%(name)s","%(levelname)-8s","%(filename)-10s","%(funcName)-15s","%(lineno)-3d","%(message)s"'
    )
    with open("logfile.csv", "w", encoding="utf-8") as f:
        f.write('"timestamp","name","level","filename","function","lineno","message"\n')
    handler = logging.FileHandler("logfile.csv", mode="a+", encoding="utf-8")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    log_config = {
        "debug": (logging.DEBUG, logger.debug),
        "info": (logging.INFO, logger.info),
        "warning": (logging.WARNING, logger.warning),
        "error": (logging.ERROR, logger.error),
        "critical": (logging.CRITICAL, logger.critical),
    }

    for level_name, (level, original_function) in log_config.items():
        setattr(
            logger,
            level_name,
            lambda message, level=level, original_function=original_function: log_message(
                level, message, max_length, original_function
            ),
        )
    return logger
