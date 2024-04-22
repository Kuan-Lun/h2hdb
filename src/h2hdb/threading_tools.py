import threading
from threading import Thread

# import os
# import psutil  # type: ignore
# import time

# from .logger import logger

SEMAPHORE = threading.Semaphore(5)
SQL_SEMAPHORE = threading.Semaphore(5)


def add_semaphore_control(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        SEMAPHORE.acquire()
        fun(*args, **kwargs)
        SEMAPHORE.release()

    return wrapper


def add_semaphore_control_for_SQL(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        SQL_SEMAPHORE.acquire()
        fun(*args, **kwargs)
        SQL_SEMAPHORE.release()

    return wrapper


class SQLThread(Thread):
    def __init__(self, target, args):
        super().__init__(target=add_semaphore_control_for_SQL(target), args=args)
