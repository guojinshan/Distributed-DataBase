from typing import Any, Dict, Hashable

from key_value_part1.readonly.crud_storage import (
    BaseCrudStorage,
    BaseShardedCrudStorage,
)
from key_value_part1.readonly.exceptions import (
    CrudKeyAlreadyExistsException,
    CrudMissingKeyException,
)


class SimpleCrudStorage(BaseCrudStorage):
    def __init__(self) -> None:
        self._content: Dict[Hashable, Any] = {}

    def create(self, key: Hashable, value: Any) -> None:
        if key not in self._content:
            self._content[key] = value
        else:
            raise CrudKeyAlreadyExistsException()

    def read(self, key: Hashable) -> Any:
        if key in self._content:
            return self._content[key]
        else:
            raise CrudMissingKeyException()

    def update(self, key: Hashable, value: Any) -> None:
        if key in self._content:
            self._content[key] = value
        else:
            raise CrudMissingKeyException()

    def delete(self, key: Hashable) -> None:
        if key in self._content:
            del self._content[key]
        else:
            raise CrudMissingKeyException()
                            

class SimpleShardedCrudStorage(BaseShardedCrudStorage):
    shard_crud_storage_cls = SimpleCrudStorage

    def create(self, key: Hashable, value: Any) -> None:
        shard = self._shards[hash(key) % self._nb_shards]
        shard.create(key, value)

    def read(self, key: Hashable) -> Any:
        shard = self._shards[hash(key) % self._nb_shards]
        return shard.read(key)

    def update(self, key: Hashable, value: Any) -> None:
        shard = self._shards[hash(key) % self._nb_shards]
        shard.update(key, value)

    def delete(self, key: Hashable) -> None:
        shard = self._shards[hash(key) % self._nb_shards]
        shard.delete(key)
