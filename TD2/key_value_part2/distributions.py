import matplotlib.pyplot as plt
import numpy as np

from key_value_part2.consistent_crud_storage import ConsistentHashingCrudStorage


if __name__ == "__main__":
    # This is a really simple script which plot histograms.
    # You should probably change this.
    Q_range = np.logspace(1, 3, 4, dtype=int)
    N = 10000
    fig, axs = plt.subplots(2, 2, figsize=(16, 9))
    axs = axs.ravel()
    fig.suptitle("Evolution de la distribution empirique de la charge(N=10000)")
    for i, ax in enumerate(axs):
        storage = ConsistentHashingCrudStorage(Q=Q_range[i], N=N)
        loads = storage.shards_load / N  # type: ignore
        # See the documentation of those to pimp your report!
        ax.hist(loads)
        ax.set_xlabel("Charge(Q={})".format(Q_range[i]))
        ax.set_ylabel("Frequence")
    plt.tight_layout()
    plt.savefig("Observation.png")
