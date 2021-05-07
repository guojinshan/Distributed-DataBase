from typing import Any, Dict, Hashable, Set

from key_value_part2.readonly.crud_storage import (
    BaseCrudStorage,
    BaseShardedCrudStorage,
)
from key_value_part2.readonly.exceptions import (
    CrudKeyAlreadyExistsException,
    CrudMissingKeyException,
)


class SimpleCrudStorage(BaseCrudStorage):
    def __init__(self) -> None:
        self._content: Dict[Hashable, Any] = {}

    def create(self, key: Hashable, value: Any) -> None:
        if key in self._content:
            raise CrudKeyAlreadyExistsException()
        self._content[key] = value

    def read(self, key: Hashable) -> Any:
        try:
            return self._content[key]
        except KeyError as err:
            raise CrudMissingKeyException() from err

    def update(self, key: Hashable, value: Any) -> None:
        if key not in self._content:
            raise CrudMissingKeyException()
        self._content[key] = value

    def delete(self, key: Hashable) -> None:
        if key not in self._content:
            raise CrudMissingKeyException()
        self._content.pop(key)

    def keys(self) -> Set[Hashable]:
        return set(self._content.keys())


class SimpleShardedCrudStorage(BaseShardedCrudStorage):
    shard_crud_storage_cls = SimpleCrudStorage

    def _key_shard_index(self, key: Hashable) -> int:
        return hash(key) % self.nb_shards

    def create(self, key: Hashable, value: Any) -> None:
        self._shards[self._key_shard_index(key)].create(key, value)

    def read(self, key: Hashable) -> Any:
        return self._shards[self._key_shard_index(key)].read(key)

    def update(self, key: Hashable, value: Any) -> None:
        self._shards[self._key_shard_index(key)].update(key, value)

    def delete(self, key: Hashable) -> None:
        self._shards[self._key_shard_index(key)].delete(key)

    def keys(self) -> Set[Hashable]:
        return set(e for shard in self._shards for e in shard.keys())
