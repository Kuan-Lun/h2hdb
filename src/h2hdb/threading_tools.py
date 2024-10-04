import threading
from threading import Thread
from abc import ABCMeta, abstractmethod
from multiprocessing import cpu_count
from multiprocessing.pool import Pool

POOL_CPU_LIMIT = max(cpu_count() - 2, 1)

MAX_THREADS = threading.Semaphore(2 * POOL_CPU_LIMIT)
SQL_SEMAPHORE = threading.Semaphore(POOL_CPU_LIMIT)


def wrap_thread_target_with_semaphores(target, semaphores: list[threading.Semaphore]):
    def wrapper(*args, **kwargs):
        MAX_THREADS.acquire(MAX_THREADS)
        for semaphore in semaphores:
            semaphore.acquire(semaphore)

        try:
            target(*args, **kwargs)
        except Exception as e:
            print(f"Thread target raised an exception: {e}")
            raise e
        finally:
            for semaphore in semaphores:
                semaphore.release()
            MAX_THREADS.release()

    return wrapper


class ThreadsList(list, metaclass=ABCMeta):
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
        for thread in self:
            thread.start()
        for thread in self:
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
