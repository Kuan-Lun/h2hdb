import threading
from threading import Thread
from abc import ABCMeta, abstractmethod


from .logger import logger

IMZGE_SEMAPHORE = threading.Semaphore(5)
KOMGA_SEMAPHORE = threading.Semaphore(5)
SQL_SEMAPHORE = threading.Semaphore(5)


class BackgroundTaskThread(Thread, metaclass=ABCMeta):
    @property
    @abstractmethod
    def semaphore(self):
        pass

    def __init__(self, target, args) -> None:
        super().__init__(
            target=self.add_semaphore_control_to_operation(target), args=args
        )

    @classmethod
    def add_semaphore_control_to_operation(cls, fun):
        def wrapper(*args, **kwargs):
            cls.semaphore.acquire()
            try:
                fun(*args, **kwargs)
            except BaseException as e:
                logger.error(f"Error in background task: {e}")
            cls.semaphore.release()

        return wrapper


class ThreadsList(list, metaclass=ABCMeta):
    @property
    @abstractmethod
    def LocalBackgroundTaskThread(self):
        pass

    def start_all(self: list[BackgroundTaskThread]):
        for thread in self:
            thread.start()

    def join_all(self: list[BackgroundTaskThread]):
        for thread in self:
            thread.join()

    def append(self, target, args):
        super().append(self.LocalBackgroundTaskThread(target=target, args=args))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.start_all()
        self.join_all()


class ImageThreadsList(ThreadsList):
    class LocalBackgroundTaskThread(BackgroundTaskThread):
        def semaphore(self):
            return IMZGE_SEMAPHORE


class KomgaThreadsList(ThreadsList):
    class LocalBackgroundTaskThread(BackgroundTaskThread):
        def semaphore(self):
            return KOMGA_SEMAPHORE


class SQLThreadsList(ThreadsList):
    class LocalBackgroundTaskThread(BackgroundTaskThread):
        def semaphore(self):
            return SQL_SEMAPHORE
