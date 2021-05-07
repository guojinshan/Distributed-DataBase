import hashlib
from typing import Any, Hashable, List

import numpy as np

from key_value_part2.simple_crud_storage import SimpleShardedCrudStorage


class ConsistentHashingCrudStorage(SimpleShardedCrudStorage):
    def __init__(self, *, Q: int = 10, N: int = 10000):
        # NOTE: The following line calls the constructor of the base class
        # (BaseShardedCrudStorage).
        # It which create and insert shards in the private list _shards also
        # defined at the level of this base class.
        super().__init__(nb_shards=Q)

        # the size the circular buffer
        self._N = N

        shards_pos = np.array([self._shard_hash(i) for i in range(self.nb_shards)])

        # We maintain two data-structures here which are sorted w.r.t
        # the shards' hashes
        self._sorted_shard_ids = np.argsort(shards_pos)
        self._sorted_shard_hashs = shards_pos[self._sorted_shard_ids]

        shards_load = self.shards_load
        self._hash_to_shard_id = np.roll(
            np.array(
                [
                    self._sorted_shard_ids[i]
                    for i in range(self.nb_shards)
                    for _ in range(shards_load[i])
                ]
            ),
            self._sorted_shard_hashs[0],
        )

    def _hash_func(self, value: Any):
        return int(hashlib.sha1(str(value).encode("utf-8")).hexdigest(), 16)

    def _shard_hash(self, i: Any) -> int:
        return self._hash_func(i) % self._N

    def _key_to_hash(self, key: Hashable) -> int:
        return self._hash_func(key) % self._N

    def _key_shard_index(self, key: Hashable) -> int:
        hash = self._key_to_hash(key)
        shard_id = self._hash_to_shard_id[hash]
        return shard_id

    @property
    def shards_load(self) -> List[int]:
        first_shard_load = (
            self._sorted_shard_hashs[0] + self._N - self._sorted_shard_hashs[-1]
        )
        other_shards_load = np.diff(self._sorted_shard_hashs)
        shards_load: List[int] = np.insert(
            other_shards_load, len(other_shards_load), first_shard_load
        )
        return shards_load

    @shards_load.setter
    def shards_load(self, value: Any) -> None:
        raise RuntimeError()

    @property
    def shards_hash(self) -> List[int]:
        return self._sorted_shard_hashs

    @shards_hash.setter
    def shards_hash(self, value: Any) -> None:
        raise RuntimeError()

    @property
    def shards_id(self) -> List[int]:
        return self._sorted_shard_ids

    @shards_id.setter
    def shards_id(self, value: Any) -> None:
        raise RuntimeError()

    def add_shard(self) -> None:
        # 1. Sachant que les identifiants sont attribués de manière contigüe
        # à partir de 0, trouver $p$, l'identifiant du dernier \shard.

        # 2. Instancier le nouveau \shard dans la liste privée \texttt{shards}.

        # 3. Chercher la position du \shard dans le tableau circulaire.

        # 4. Mettre à jour les deux tableaux \texttt{sorted\_shard\_ids}
        # et \texttt{sorted\_shard\_hashs} en conséquence.

        # 5. Récupérer les \shards le précédent et le suivant en récupérant
        # d'abord leurs identifiants.

        # 6. Trouver la plage de \hashs à attribuer à ce nouveau \shard.

        # 7. Mettre à jour le tableau \texttt{hash\_to\_shard\_id}
        # en conséquence.

        # 8. Effectuer la migration de  éléments du précédent \shard
        # contenus dans cette plage vers le nouveau \shard.
        raise NotImplementedError()

    def remove_shard(self) -> None:
        # Please, if you comment, do comment on the motives of your code
        # (i.e why your code does what it does) and on how it does (if it
        # is not easy to understand from the code.
        raise NotImplementedError()
