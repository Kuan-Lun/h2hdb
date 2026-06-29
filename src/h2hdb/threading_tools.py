from collections.abc import Callable
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from typing import Any

CPU_NUM = cpu_count()
POOL_CPU_LIMIT = max(CPU_NUM - 2, 1)


def run_in_parallel(fun: Callable[..., Any], args: list[tuple[Any, ...]]) -> list[Any]:
    results: list[Any] = list()
    if args:
        with Pool(POOL_CPU_LIMIT) as pool:
            if len(args[0]) > 1:
                results += pool.starmap(fun, args)
            else:
                results += pool.map(fun, [arg[0] for arg in args])
    return results
