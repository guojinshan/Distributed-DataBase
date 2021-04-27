from abc import ABC
from typing import Any, Dict, Type

import pytest

from key_value_part1.readonly.crud_storage import BaseCrudStorage
from key_value_part1.readonly.exceptions import (
    CrudKeyAlreadyExistsException,
    CrudMissingKeyException,
)


class CrudCommonTests(ABC):
    storage_cls: Type[BaseCrudStorage]
    storage_cls_kwargs: Dict[str, Any] = None

    _storage: BaseCrudStorage

    def setup_method(self, method):
        """setup any state tied to the execution of the given method in a
        class.  setup_method is invoked for every test method of a class.
        """
        storage_cls_kwargs = (
            {} if self.storage_cls_kwargs is None else self.storage_cls_kwargs
        )
        self._storage = self.storage_cls(**storage_cls_kwargs)

    def test_simple_crud_create_read(self):
        self._storage.create("a", "a")
        assert self._storage.read("a") == "a"

    def test_create_cannot_update(self):
        self._storage.create("a", "a")
        with pytest.raises(CrudKeyAlreadyExistsException):
            self._storage.create("a", "a")

    def test_read_non_existent_key(self):
        with pytest.raises(CrudMissingKeyException):
            self._storage.read("a")

    def test_update_key(self):
        self._storage.create("a", 1)
        self._storage.update("a", 2)
        assert self._storage.read("a") == 2

    def test_cannot_update_missing_key(self):
        with pytest.raises(CrudMissingKeyException):
            self._storage.update("a", 2)

    def test_delete_key(self):
        self._storage.create("a", 1)
        assert self._storage.read("a") == 1
        self._storage.delete("a")
        with pytest.raises(CrudMissingKeyException):
            self._storage.read("a")

    def test_cannot_delete_missing_key(self):
        with pytest.raises(CrudMissingKeyException):
            self._storage.delete("a")

    def test_scenario(self):
        with pytest.raises(CrudMissingKeyException):
            self._storage.read("toto")
        self._storage.create("toto", 1)

        with pytest.raises(CrudKeyAlreadyExistsException):
            self._storage.create("toto", 2)
        assert self._storage.read("toto") == 1

        self._storage.update("toto", 2)
        assert self._storage.read("toto") == 2

        with pytest.raises(CrudMissingKeyException):
            self._storage.read("tutu")

        self._storage.delete("toto")
        with pytest.raises(CrudMissingKeyException):
            self._storage.read("toto")

        with pytest.raises(CrudMissingKeyException):
            self._storage.delete("toto")
