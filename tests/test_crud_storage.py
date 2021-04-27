import pytest

from key_value_part1.simple_crud_storage import (
    SimpleCrudStorage,
    SimpleShardedCrudStorage,
)
from tests.utils import CrudCommonTests


class TestSimpleCrudStorage(CrudCommonTests):
    storage_cls = SimpleCrudStorage


class TestSimpleShardedCrudStorage(CrudCommonTests):
    storage_cls = SimpleShardedCrudStorage
    storage_cls_kwargs = {"nb_shards": 10}


def test_nb_shards_must_be_valid():
    with pytest.raises(AssertionError):
        SimpleShardedCrudStorage(nb_shards=0)

    with pytest.raises(AssertionError):
        SimpleShardedCrudStorage(nb_shards=-1)

    SimpleShardedCrudStorage(nb_shards=1)


@pytest.mark.parametrize("nb_shards", range(1, 10))
def test_data_is_actually_sharded(nb_shards: int):
    storage = SimpleShardedCrudStorage(nb_shards=nb_shards)

    for i in range(nb_shards):
        storage.create(i, i)

    for i, shard in enumerate(storage._shards):
        assert shard.read(i) == i
