import matplotlib.pyplot as plt
import numpy as np

from key_value_part2.consistent_crud_storage import ConsistentHashingCrudStorage


if __name__ == "__main__":
    # This is a really simple script which plot histograms.
    # You should probably change this.
    Q_range = np.logspace(1, 3, 4, dtype=int)
    N = 10000
    for Q in Q_range:
        storage = ConsistentHashingCrudStorage(Q=Q, N=N)
        loads = storage.shards_load / N  # type: ignore
        plt.figure(figsize=(16, 9))
        # See the documentation of those to pimp your report!
        plt.hist(loads)
        plt.xlim(0, 1)
        plt.title("")
        plt.xlabel("")
        plt.ylabel("")
        plt.show()
