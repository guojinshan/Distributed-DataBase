from abc import ABC, abstractmethod
from typing import Any, Hashable, Tuple, Type


class BaseCrudStorage(ABC):
    @abstractmethod
    def create(self, key: Hashable, value: Any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def read(self, key: Hashable) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def update(self, key: Hashable, value: Any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete(self, key: Hashable) -> None:
        raise NotImplementedError()


class BaseShardedCrudStorage(BaseCrudStorage, ABC):
    shard_crud_storage_cls: Type[BaseCrudStorage]

    def __init__(self, *, nb_shards: int) -> None:
        self._nb_shards = nb_shards
        assert self._nb_shards >= 1
        self._shards: Tuple[BaseCrudStorage, ...] = tuple(
            self.shard_crud_storage_cls() for _ in range(nb_shards)
        )
