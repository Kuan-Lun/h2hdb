import threading
from threading import Thread
from abc import ABCMeta, abstractmethod
from typing import Callable
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from contextlib import ExitStack

CPU_NUM = cpu_count()
POOL_CPU_LIMIT = max(CPU_NUM - 2, 1)

MAX_THREADS = 2 * CPU_NUM
SQL_SEMAPHORE = threading.Semaphore(POOL_CPU_LIMIT)


def wrap_thread_target_with_semaphores(
    target: Callable,
    semaphores: list[threading.Semaphore],
) -> Callable:
    def wrapper(*args, **kwargs) -> None:
        with ExitStack() as stack:
            for semaphore in semaphores:
                stack.enter_context(semaphore)
            target(*args, **kwargs)

    return wrapper


class ThreadsList(list[Thread], metaclass=ABCMeta):
    @abstractmethod
    def get_semaphores(self) -> list[threading.Semaphore]:
        pass

    def append(self, target, args):
        thread = Thread(
            target=wrap_thread_target_with_semaphores(target, self.get_semaphores()),
            args=args,
        )
        super().append(thread)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        running_threads = list[Thread]()
        while self:
            self[0].start()
            running_threads.append(self.pop(0))
            while len(running_threads) >= MAX_THREADS:
                for thread in running_threads:
                    if not thread.is_alive():
                        thread.join()
                        running_threads.remove(thread)
        for thread in running_threads:
            thread.join()


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
