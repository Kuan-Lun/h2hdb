import threading
from threading import Thread
from abc import ABCMeta, abstractmethod


from .logger import logger

IMZGE_SEMAPHORE = threading.Semaphore(5)
KOMGA_SEMAPHORE = threading.Semaphore(5)
SQL_SEMAPHORE = threading.Semaphore(5)


class BackgroundTaskThread(Thread, metaclass=ABCMeta):
    @abstractmethod
    def semaphore(self) -> threading.Semaphore:
        pass

    def __init__(self, target, args) -> None:
        super().__init__(
            target=self.add_semaphore_control_to_operation(target), args=args
        )

    def add_semaphore_control_to_operation(self, fun):
        def wrapper(*args, **kwargs):
            self.semaphore().acquire()
            try:
                fun(*args, **kwargs)
            except BaseException as e:
                logger.error(f"Error in background task: {e}")
            self.semaphore().release()

        return wrapper


class ThreadsList(list, metaclass=ABCMeta):
    class LocalBackgroundTaskThread(BackgroundTaskThread, metaclass=ABCMeta):
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
        def semaphore(self) -> threading.Semaphore:
            return IMZGE_SEMAPHORE


class KomgaThreadsList(ThreadsList):
    class LocalBackgroundTaskThread(BackgroundTaskThread):
        def semaphore(self) -> threading.Semaphore:
            return KOMGA_SEMAPHORE


class SQLThreadsList(ThreadsList):
    class LocalBackgroundTaskThread(BackgroundTaskThread):
        def semaphore(self) -> threading.Semaphore:
            return SQL_SEMAPHORE
