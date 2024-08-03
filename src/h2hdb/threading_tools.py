import threading
from threading import Thread
from abc import ABCMeta, abstractmethod
from multiprocessing import cpu_count
from multiprocessing.pool import Pool

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

    def __init__(self, target, args) -> None:
        super().__init__(
            target=self.add_semaphore_control_to_operation(target), args=args
        )

    def add_semaphore_control_to_operation(self, fun):
        def wrapper(*args, **kwargs):
            for semaphore in self.get_semaphores():
                semaphore.acquire()
            try:
                fun(*args, **kwargs)
            except BaseException as e:
                logger.error(f"Error in background task: {e}")
            for semaphore in self.get_semaphores():
                semaphore.release()

        return wrapper


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


def run_in_parallel(fun, args: list[tuple]):
    if len(args) == 0:
        return

    with Pool(POOL_CPU_LIMIT) as pool:
        if len(args[0]) > 1:
            pool.starmap(fun, args)
        else:
            pool.map(fun, args)
