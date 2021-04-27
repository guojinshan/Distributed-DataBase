from threading import Lock
from time import sleep
from typing import Any, Hashable

from key_value_part1.simple_crud_storage import (
    SimpleCrudStorage,
    SimpleShardedCrudStorage,
)


class SimpleIOBoundedCrudStorage(SimpleCrudStorage):
    def __init__(self) -> None:
        super().__init__()
        self._lock = Lock()

    def _persist_content(self) -> None:
        # Fake io bound, make sure threads really wait
        with self._lock:
            sleep(0.0002)

    def _load_persisted_content(self) -> None:
        # Fake io bound, make sure threads really wait
        with self._lock:
            sleep(0.0001)

    def create(self, key: Hashable, value: Any) -> None:
        self._load_persisted_content()
        super().create(key, value)
        self._persist_content()

    def read(self, key: Hashable) -> Any:
        self._load_persisted_content()
        return super().read(key)

    def update(self, key: Hashable, value: Any) -> None:
        self._load_persisted_content()
        super().update(key, value)
        self._persist_content()

    def delete(self, key: Hashable) -> None:
        self._load_persisted_content()
        super().delete(key)
        self._persist_content()


class SimpleIOBoundedShardedCrudStorage(SimpleShardedCrudStorage):
    shard_crud_storage_cls = SimpleIOBoundedCrudStorage
