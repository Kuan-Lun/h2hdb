import threading
from threading import Thread
from abc import ABCMeta, abstractmethod


from .logger import logger

CBZ_SEMAPHORE = threading.Semaphore(2)
KOMGA_SEMAPHORE = threading.Semaphore(5)
SQL_SEMAPHORE = threading.Semaphore(5)


class ThreadsList(list, metaclass=ABCMeta):
    @property
    @abstractmethod
    def semaphore(self):
        pass

    class BackgroundTaskThread(Thread, metaclass=ABCMeta):

        @staticmethod
        def add_semaphore_control_to_operation(fun):
            def wrapper(*args, **kwargs):
                ThreadsList.semaphore.acquire()
                try:
                    fun(*args, **kwargs)
                except BaseException as e:
                    logger.error(f"Error in background task: {e}")
                ThreadsList.semaphore.release()

            return wrapper

    def start_all(self: list[BackgroundTaskThread]):
        for thread in self:
            thread.start()

    def join_all(self: list[BackgroundTaskThread]):
        for thread in self:
            thread.join()
        del self

    def append(self, target, args):
        super().append(self.BackgroundTaskThread(target=target, args=args))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.start_all()
        self.join_all()


class CBZThreadsList(ThreadsList):
    def semaphore(self):
        return CBZ_SEMAPHORE


class KomgaThreadsList(ThreadsList):
    def semaphore(self):
        return KOMGA_SEMAPHORE


class SQLThreadsList(ThreadsList):
    def semaphore(self):
        return SQL_SEMAPHORE
