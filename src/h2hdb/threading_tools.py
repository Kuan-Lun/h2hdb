import threading
from threading import Thread
from abc import ABCMeta, abstractmethod
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from contextlib import ExitStack

from .logger import logger

POOL_CPU_LIMIT = max(cpu_count() - 2, 1)

# MAX_IO_SEMAPHORE = threading.Semaphore(5)
# CBZ_IO_SEMAPHORE = threading.Semaphore(3)
KOMGA_SEMAPHORE = threading.Semaphore(5)
SQL_SEMAPHORE = threading.Semaphore(10)


class BackgroundTaskThread(Thread, metaclass=ABCMeta):
    @abstractmethod
    def get_semaphores(self) -> list[threading.Semaphore]:
        pass

    def start(self) -> None:
        with ExitStack() as stack:
            for semaphore in self.get_semaphores():
                stack.enter_context(semaphore)
            try:
                super().start()
            except BaseException as e:
                logger.error(f"Error in background task: {e}")


class ThreadsList(list, metaclass=ABCMeta):
    class LocalBackgroundTaskThread(BackgroundTaskThread, metaclass=ABCMeta):
        pass

    def join_all(self: list[BackgroundTaskThread]):
        for thread in self:
            thread.join()

    def append(self, target, args):
        thread = self.LocalBackgroundTaskThread(target=target, args=args)
        thread.start()
        super().append(thread)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.join_all()


# class CBZThreadsList(ThreadsList):
#     class LocalBackgroundTaskThread(BackgroundTaskThread):
#         def get_semaphores(self) -> list[threading.Semaphore]:
#             return [CBZ_IO_SEMAPHORE, MAX_IO_SEMAPHORE]


class KomgaThreadsList(ThreadsList):
    class LocalBackgroundTaskThread(BackgroundTaskThread):
        def get_semaphores(self) -> list[threading.Semaphore]:
            return [KOMGA_SEMAPHORE]


class SQLThreadsList(ThreadsList):
    class LocalBackgroundTaskThread(BackgroundTaskThread):
        def get_semaphores(self) -> list[threading.Semaphore]:
            return [SQL_SEMAPHORE]


def run_in_parallel(fun, args: list[tuple]) -> list:
    if len(args) == 0:
        return list()

    with Pool(POOL_CPU_LIMIT) as pool:
        if len(args[0]) > 1:
            results = pool.starmap(fun, args)
        else:
            results = pool.map(fun, [arg[0] for arg in args])
    return results
