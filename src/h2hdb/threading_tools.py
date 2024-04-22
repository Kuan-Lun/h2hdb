import threading

# import os
# import psutil  # type: ignore
# import time

# from .logger import logger

SEMAPHORE = threading.Semaphore(5)


def add_semaphore_control(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        SEMAPHORE.acquire()
        fun(*args, **kwargs)
        SEMAPHORE.release()

    return wrapper
