import threading
from threading import Thread

# import os
# import psutil  # type: ignore
# import time

# from .logger import logger

CBZ_SEMAPHORE = threading.Semaphore(1)
KOMGA_SEMAPHORE = threading.Semaphore(5)
SQL_SEMAPHORE = threading.Semaphore(5)


def add_semaphore_control_to_cbz_compression_operation(fun):
    def wrapper(*args, **kwargs):
        CBZ_SEMAPHORE.acquire()
        fun(*args, **kwargs)
        CBZ_SEMAPHORE.release()

    return wrapper


class CBZThread(Thread):
    def __init__(self, target, args):
        super().__init__(
            target=add_semaphore_control_to_cbz_compression_operation(target), args=args
        )


def add_semaphore_control_to_komga_operation(fun):
    def wrapper(*args, **kwargs):
        KOMGA_SEMAPHORE.acquire()
        fun(*args, **kwargs)
        KOMGA_SEMAPHORE.release()

    return wrapper


class KomgaThread(Thread):
    def __init__(self, target, args):
        super().__init__(
            target=add_semaphore_control_to_komga_operation(target), args=args
        )


def add_semaphore_control_to_SQL_operation(fun):
    def wrapper(*args, **kwargs):
        SQL_SEMAPHORE.acquire()
        fun(*args, **kwargs)
        SQL_SEMAPHORE.release()

    return wrapper


class SQLThread(Thread):
    def __init__(self, target, args):
        super().__init__(
            target=add_semaphore_control_to_SQL_operation(target), args=args
        )
