import threading

SEMAPHORE = threading.Semaphore(5)


def add_semaphore_control(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        SEMAPHORE.acquire()
        try:
            fun(*args, **kwargs)
        finally:
            SEMAPHORE.release()

    return wrapper
