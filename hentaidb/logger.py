import logging
import unicodedata


def is_cjk(char: str):
    return "CJK UNIFIED" in unicodedata.name(char, "")


def str_len(s: str):
    length = 0
    for char in s:
        if is_cjk(char):
            length += 2
        else:
            length += 1
    return length


def split_message(message: str, max_length: int):
    chunks = []
    chunk = ""
    chunk_len = 0
    for char in message:
        if len(chunk) > 0 and chunk[0] == " ":
            chunk = chunk[1:]
        char_len = 2 if is_cjk(char) else 1
        if chunk_len + char_len > max_length:
            chunks.append(chunk)
            chunk = ""
            chunk_len = 0
        chunk += char
        chunk_len += char_len
    if chunk:  # don't forget to append the last chunk
        chunks.append(chunk)
    return chunks


def setup_logger(level: int = logging.DEBUG, max_length: int = 30) -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(
        level
    )  # logging.DEBUG or logging.INFO or logging.WARNING or logging.ERROR or logging.CRITICAL
    formatter = logging.Formatter(
        "%(asctime)s %(name)-6s %(levelname)-8s %(filename)-10s %(funcName)-15s %(lineno)-3d %(message)s"
    )
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 保存原始的日志方法
    original_debug = logger.debug
    original_info = logger.info
    original_warning = logger.warning
    original_error = logger.error
    original_critical = logger.critical

    # 定义一个嵌套函数，用于处理日志消息的分行
    def log_message(level: int, message: str):
        chunks = split_message(message, max_length)
        for chunk in chunks:
            if level == logging.DEBUG:
                original_debug(chunk)
            elif level == logging.INFO:
                original_info(chunk)
            elif level == logging.WARNING:
                original_warning(chunk)
            elif level == logging.ERROR:
                original_error(chunk)
            elif level == logging.CRITICAL:
                original_critical(chunk)

    # 替换原来的 logger.info, logger.debug 等方法
    logger.info = lambda message: log_message(logging.INFO, message)
    logger.debug = lambda message: log_message(logging.DEBUG, message)
    logger.warning = lambda message: log_message(logging.WARNING, message)
    logger.error = lambda message: log_message(logging.ERROR, message)
    logger.critical = lambda message: log_message(logging.CRITICAL, message)
    return logger


# logger = setup_logger(logging.INFO)
# logger.debug("這是一條 debug 級別的日誌")
# logger.info("這是一條 info 級別的日誌")
# logger.warning("這是一條 warning 級別的日誌")
# logger.error("這是一條 error 級別的日誌")
# logger.critical("這是一條 critical 級別的日誌")
