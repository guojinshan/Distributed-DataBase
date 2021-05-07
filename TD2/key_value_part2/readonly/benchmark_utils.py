import time
from functools import wraps
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from typing import Callable, Hashable, List, Tuple

from key_value_part2.readonly.crud_storage import BaseCrudStorage


CPU_COUNT = cpu_count()


def get_execution_time(f: Callable) -> Callable:  # type: ignore
    @wraps(f)
    def wrap(*args, **kwargs) -> float:  # type: ignore
        ts = time.perf_counter()
        f(*args, **kwargs)
        te = time.perf_counter()
        return te - ts

    return wrap


def _write_to_storage(args: Tuple[BaseCrudStorage, Hashable]) -> None:
    storage, key_value = args
    storage.create(key_value, key_value)


def _read_from_storage(args: Tuple[BaseCrudStorage, Hashable]) -> None:
    storage, key_value = args
    assert storage.read(key_value) == key_value


@get_execution_time
def benchmark_mono_thread_write(
    storage: BaseCrudStorage, key_values: List[Hashable]
) -> None:
    for key_value in key_values:
        _write_to_storage((storage, key_value))


@get_execution_time
def benchmark_mono_thread_read(
    storage: BaseCrudStorage, key_values: List[Hashable]
) -> None:
    for key_value in key_values:
        _read_from_storage((storage, key_value))


@get_execution_time
def benchmark_multi_thread_write(
    storage: BaseCrudStorage, key_values: List[Hashable]
) -> None:
    with ThreadPool(CPU_COUNT * 2 - 1) as pool:
        pool.map(
            _write_to_storage,
            [(storage, key_value) for key_value in key_values],
        )


@get_execution_time
def benchmark_multi_thread_read(
    storage: BaseCrudStorage, key_values: List[Hashable]
) -> None:
    with ThreadPool(CPU_COUNT * 2 - 1) as pool:
        pool.map(
            _read_from_storage,
            [(storage, key_value) for key_value in key_values],
        )
