import threading
from threading import Thread
from abc import ABCMeta, abstractmethod
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from contextlib import ExitStack

POOL_CPU_LIMIT = max(cpu_count() - 2, 1)

# MAX_IO_SEMAPHORE = threading.Semaphore(5)
# CBZ_IO_SEMAPHORE = threading.Semaphore(3)
KOMGA_SEMAPHORE = threading.Semaphore(POOL_CPU_LIMIT)
SQL_SEMAPHORE = threading.Semaphore(POOL_CPU_LIMIT)


def wrap_thread_target_with_semaphores(target, get_semaphores):
    def wrapper(*args, **kwargs):
        with ExitStack() as stack:
            for semaphore in get_semaphores():
                stack.enter_context(semaphore)
            target(*args, **kwargs)

    return wrapper


class ThreadsList(list, metaclass=ABCMeta):
    @abstractmethod
    def get_semaphores(self) -> list[threading.Semaphore]:
        pass

    def append(self, target, args):
        thread = Thread(
            target=wrap_thread_target_with_semaphores(target, self.get_semaphores),
            args=args,
        )
        super().append(thread)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for thread in self:
            thread.start()
        for thread in self:
            thread.join()


# class CBZThreadsList(ThreadsList):
#     def get_semaphores(self) -> list[threading.Semaphore]:
#         return [CBZ_IO_SEMAPHORE, MAX_IO_SEMAPHORE]


class KomgaThreadsList(ThreadsList):
    def get_semaphores(self) -> list[threading.Semaphore]:
        return [KOMGA_SEMAPHORE]


class SQLThreadsList(ThreadsList):
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
