from key_value_part1.readonly.crud_storage_io_bounded import (
    SimpleIOBoundedCrudStorage,
    SimpleIOBoundedShardedCrudStorage,
)
from tests.utils import CrudCommonTests


class TestSimplePersistedCrudStorage(CrudCommonTests):
    storage_cls = SimpleIOBoundedCrudStorage


class TestSimplePersistedShardedCrudStorage(CrudCommonTests):
    storage_cls = SimpleIOBoundedShardedCrudStorage
    storage_cls_kwargs = {"nb_shards": 10}
