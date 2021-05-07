from typing import Any, Hashable, List

import numpy as np

from key_value_part2.simple_crud_storage import SimpleShardedCrudStorage


class ConsistentHashingCrudStorage(SimpleShardedCrudStorage):
    def __init__(self, *, Q: int, N: int = 1337):
        super().__init__(nb_shards=Q)
        # the range for the positions
        self._N = N
        shards_pos = np.array([self._shard_hash(i) for i in range(self.nb_shards)])

        # We maintain data-structure here which are sorted w.r.t the shard
        # hashes
        self._sorted_shard_ids: List[int] = np.argsort(shards_pos)
        self._sorted_shard_hashs: List[int] = shards_pos[self._sorted_shard_ids]

        shards_load = self.shards_load
        self._hash_to_shard_id: List[int] = np.array(
            [
                self._sorted_shard_ids[i]
                for i in range(self.nb_shards)
                for _ in range(shards_load[i])
            ]
        )

    def _shard_hash(self, i: Any) -> int:
        # We use a string because hash(i) == i for small integer i
        return hash(str(41 * i + 17)) % self._N

    def _key_to_hash(self, key: Hashable) -> int:
        return hash(key) % self._N

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
        shards_add_id = self.nb_shards
        shards_add_hash = self._shard_hash(shards_add_id)

        # 2. Instancier le nouveau \shard dans la liste privée \texttt{shards}.
        self._shards.append(self.shard_crud_storage_cls())

        # 3. Chercher la position du \shard dans le tableau circulaire.
        shards_pos_tab = (
            shards_add_hash - self._sorted_shard_hashs[0]
            if shards_add_hash >= self._sorted_shard_hashs[0]
            else self._N - self._sorted_shard_hashs[0] + shards_add_hash
        )

        # 4. Mettre à jour les deux tableaux \texttt{sorted\_shard\_ids}
        # et \texttt{sorted\_shard\_hashs} en conséquence.
        shards_add_pos = np.searchsorted(self._sorted_shard_hashs, shards_add_hash)
        self._sorted_shard_ids = np.insert(
            self._sorted_shard_ids, shards_add_pos, shards_add_id
        )
        self._sorted_shard_hashs = np.insert(
            self._sorted_shard_hashs, shards_add_pos, shards_add_hash
        )

        # 5. Récupérer les \shards le précédent et le suivant en récupérant
        # d'abord leurs identifiants.
        if shards_add_pos == 0:
            pre_shards_hash = self._sorted_shard_hashs[shards_add_id]
            post_shards_hash = self._sorted_shard_hashs[shards_add_pos + 1]
        elif shards_add_pos == shards_add_id:
            pre_shards_hash = self._sorted_shard_hashs[shards_add_pos - 1]
            post_shards_hash = self._sorted_shard_hashs[0]
        else:
            pre_shards_hash = self._sorted_shard_hashs[shards_add_pos - 1]
            post_shards_hash = self._sorted_shard_hashs[shards_add_pos + 1]

        # 6. Trouver la plage de \hashs à attribuer à ce nouveau \shard.
        shards_add_load = (
            post_shards_hash - shards_add_hash
            if post_shards_hash >= shards_add_hash
            else self._N - shards_add_hash + post_shards_hash
        )

        # 7. Mettre à jour le tableau \texttt{hash\_to\_shard\_id}
        # en conséquence.
        for i in np.arange(shards_add_load):
            self._hash_to_shard_id[shards_pos_tab + i] = shards_add_id

        # 8. Effectuer la migration de  éléments du précédent \shard
        # contenus dans cette plage vers le nouveau \shard.
        shard_index = self._sorted_shard_ids[
            list(self._sorted_shard_hashs).index(pre_shards_hash)
        ]
        for key in self._shards[shard_index].keys():
            self._shards[shards_add_id].create(key, self._shards[shard_index].read(key))

    def remove_shard(self) -> None:
        # Please, if you comment, do comment on the motives of your code
        # (i.e why your code does what it does) and on how it does (if it
        # is not easy to understand from the code.

        # 1. Obtenir l'indentifiant du dernier \shard
        shards_remove_id = np.max(self._sorted_shard_ids)
        shards_remove_pos = list(self._sorted_shard_ids).index(shards_remove_id)
        shards_remove_hash = self._sorted_shard_hashs[shards_remove_pos]

        # 2. Chercher la position du \shard dans le tableau circulaire
        shards_remove_pos_tab = (
            shards_remove_hash - self._sorted_shard_hashs[0]
            if shards_remove_hash >= self._sorted_shard_hashs[0]
            else self._N - self._sorted_shard_hashs[0] + shards_remove_hash
        )

        # 3. Récupérer les \shards le précédent et le suivant en récupérant
        # d'abord leurs identifiants.
        if shards_remove_pos == 0:
            pre_shards_hash = self._sorted_shard_hashs[-1]
            post_shards_hash = self._sorted_shard_hashs[shards_remove_pos + 1]
        elif shards_remove_pos == shards_remove_id:
            pre_shards_hash = self._sorted_shard_hashs[shards_remove_pos - 1]
            post_shards_hash = self._sorted_shard_hashs[0]
        else:
            pre_shards_hash = self._sorted_shard_hashs[shards_remove_pos - 1]
            post_shards_hash = self._sorted_shard_hashs[shards_remove_pos + 1]

        # 4. Trouver la plage de \hashs attribuée à ce \shard
        shards_remove_load = (
            post_shards_hash - shards_remove_hash
            if post_shards_hash >= shards_remove_hash
            else self._N - shards_remove_hash + post_shards_hash
        )

        # 5. Mettre à jour le tableau \texttt{hash\_to\_shard\_id}
        # en conséquence.
        pre_shards_id = self._sorted_shard_ids[
            list(self._sorted_shard_hashs).index(pre_shards_hash)
        ]
        for i in np.arange(shards_remove_load):
            self._hash_to_shard_id[shards_remove_pos_tab + i] = pre_shards_id

        # 6. Effectuer la migration des éléments du contenus
        # de \shard dans la plage vers le précédent \shard.
        for key in self._shards[shards_remove_id].keys():
            self._shards[pre_shards_id].create(
                key, self._shards[shards_remove_id].read(key)
            )
            self._shards[shards_remove_id].delete(key)

        # 7. Mettre à jour les deux tableaux \texttt{sorted\_shard\_ids}
        # et \texttt{sorted\_shard\_hashs} en conséquence.
        self._sorted_shard_ids = np.delete(self._sorted_shard_ids, shards_remove_pos)
        self._sorted_shard_hashs = np.delete(
            self._sorted_shard_hashs, shards_remove_pos
        )

        # 8. Supprimer le dernier \shard dans la liste privée \texttt{shards}.
        self._shards.pop()
