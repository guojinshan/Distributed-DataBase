from abc import ABC, abstractmethod
from typing import Any, Hashable, List, Set, Type


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

    @abstractmethod
    def keys(self) -> Set[Hashable]:
        raise NotImplementedError()


class BaseShardedCrudStorage(BaseCrudStorage, ABC):
    shard_crud_storage_cls: Type[BaseCrudStorage]

    def __init__(self, *, nb_shards: int) -> None:
        assert nb_shards >= 1
        self._shards: List[BaseCrudStorage] = list(
            self.shard_crud_storage_cls() for _ in range(nb_shards)
        )

    @property
    def nb_shards(self) -> int:
        return len(self._shards)

    @nb_shards.setter
    def nb_shards(self, value: Any) -> None:
        raise RuntimeError()
