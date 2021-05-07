import numpy as np
import pytest

from key_value_part2.consistent_crud_storage import ConsistentHashingCrudStorage
from tests.utils import CrudCommonTests


class TestConsistentHashingCrudStorage(CrudCommonTests):
    storage_cls = ConsistentHashingCrudStorage
    storage_cls_kwargs = {"Q": 10, "N": 10000}


@pytest.mark.parametrize("N", [1337, 5000, 10000, 1618390])
@pytest.mark.parametrize("nb_shards", [1, 2, 5, 10, 20])
def test_consistent_hashing_crud_storage(N, nb_shards):
    storage = ConsistentHashingCrudStorage(N=N, Q=nb_shards)

    assert sum(storage.shards_load) == N
    assert len(storage.shards_load) == nb_shards
    assert np.all(0 <= storage.shards_load)
    assert np.all(storage.shards_hash < N)

    # Shard hashes must be in {0, ..., N-1}
    assert np.all(0 <= storage.shards_hash)
    assert np.all(storage.shards_hash < N)

    # Shard hashes should be sorted
    np.testing.assert_array_equal(np.argsort(storage.shards_hash), np.arange(nb_shards))

    # Testing loads against their definition
    unique, counts = np.unique(storage._hash_to_shard_id, return_counts=True)
    np.testing.assert_equal(np.sort(storage.shards_load), np.sort(counts))


@pytest.mark.parametrize("N", [1337, 1618390])
@pytest.mark.parametrize("Q", [1, 2, 5, 10, 20])
def test_add_shards_correct_containers(N, Q):
    nb_original_shards = Q
    storage = ConsistentHashingCrudStorage(N=N, Q=nb_original_shards)

    assert storage.nb_shards == nb_original_shards
    assert np.all(0 < storage.shards_load)

    for n_new_shard in range(1, 5):
        storage.add_shard()
        assert sum(storage.shards_load) == N
        assert len(storage.shards_load) == Q + n_new_shard
        assert np.all(0 <= storage.shards_load)
        assert np.all(storage.shards_hash < N)

        # Shard hashes must be in {0, ..., N-1}
        assert np.all(0 <= storage.shards_hash)
        assert np.all(storage.shards_hash < N)

        # Shard hashes should be sorted
        np.testing.assert_array_equal(
            np.argsort(storage.shards_hash), np.arange(Q + n_new_shard)
        )

        assert storage.nb_shards == nb_original_shards + n_new_shard


@pytest.mark.parametrize("N", [1337, 1618390])
@pytest.mark.parametrize("Q", [1, 2, 5, 10])
def test_add_shards_crud(N, Q):
    nb_original_shards = Q
    storage = ConsistentHashingCrudStorage(N=N, Q=nb_original_shards)

    assert storage.nb_shards == nb_original_shards

    unique, counts = np.unique(storage._hash_to_shard_id, return_counts=True)
    np.testing.assert_equal(np.sort(storage.shards_load), np.sort(counts))

    storage.create("dog", "cat")
    storage.create("prime", 41)

    storage.add_shard()
    unique, counts = np.unique(storage._hash_to_shard_id, return_counts=True)
    np.testing.assert_equal(np.sort(storage.shards_load), np.sort(counts))

    assert storage.read("prime") == 41
    assert storage.read("dog") == "cat"

    storage.update("prime", 17)
    storage.delete("dog")

    storage.add_shard()
    assert storage.read("prime") == 17


@pytest.mark.parametrize("N", [1337, 1618390])
@pytest.mark.parametrize("Q", [5, 10])
def test_remove_shards_consistent_containers(N, Q):
    nb_original_shards = Q
    storage = ConsistentHashingCrudStorage(N=N, Q=nb_original_shards)

    assert storage.nb_shards == nb_original_shards
    assert np.all(0 <= storage.shards_load)

    for n_new_shard in range(1, 5):
        storage.remove_shard()
        assert sum(storage.shards_load) == N
        assert len(storage.shards_load) == Q - n_new_shard
        assert np.all(0 <= storage.shards_load)
        assert np.all(storage.shards_hash < N)

        # Shard hashes must be in {0, ..., N-1}
        assert np.all(0 <= storage.shards_hash)
        assert np.all(storage.shards_hash < N)

        # Shard hashes should be sorted
        np.testing.assert_array_equal(
            np.argsort(storage.shards_hash), np.arange(Q - n_new_shard)
        )

        assert storage.nb_shards == nb_original_shards - n_new_shard


@pytest.mark.parametrize("N", [1337, 1618390])
@pytest.mark.parametrize("Q", [5, 10])
def test_remove_shards_crud(N, Q):
    nb_original_shards = Q
    storage = ConsistentHashingCrudStorage(N=N, Q=nb_original_shards)

    assert storage.nb_shards == nb_original_shards

    unique, counts = np.unique(storage._hash_to_shard_id, return_counts=True)
    np.testing.assert_equal(np.sort(storage.shards_load), np.sort(counts))

    storage.create("dog", "cat")
    storage.create("prime", 41)

    storage.remove_shard()
    unique, counts = np.unique(storage._hash_to_shard_id, return_counts=True)
    np.testing.assert_equal(np.sort(storage.shards_load), np.sort(counts))

    assert storage.read("prime") == 41
    assert storage.read("dog") == "cat"

    storage.update("prime", 17)
    storage.delete("dog")

    storage.remove_shard()
    assert storage.read("prime") == 17
