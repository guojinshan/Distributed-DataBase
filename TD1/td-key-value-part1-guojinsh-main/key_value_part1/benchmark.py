import random

# Import packages
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from key_value_part1.readonly.benchmark_utils import (
    benchmark_mono_thread_read,
    benchmark_mono_thread_write,
    benchmark_multi_thread_read,
    benchmark_multi_thread_write,
)
from key_value_part1.readonly.crud_storage_io_bounded import (
    SimpleIOBoundedCrudStorage,
    SimpleIOBoundedShardedCrudStorage,
)
from key_value_part1.simple_crud_storage import (
    SimpleCrudStorage,
    SimpleShardedCrudStorage,
)


if __name__ == "__main__":
    # random write / access
    N_KEYS = 10_000
    KEY_VALUES_WRITE = random.sample(range(N_KEYS), N_KEYS)
    KEY_VALUES_READ = random.sample(range(N_KEYS), N_KEYS)

    # Initialize dictionary to store the running time result for mono_thread and multi_thread
    dic_mono, dic_mult = dict(), dict()

    for step_info in [
        ("mono_thread", benchmark_mono_thread_read, benchmark_mono_thread_write),
        ("multi_thread", benchmark_multi_thread_read, benchmark_multi_thread_write),
    ]:
        title, benchmark_read, benchmark_write = step_info
        # print(title)

        storages = (
            SimpleCrudStorage(),
            SimpleShardedCrudStorage(nb_shards=10),
            SimpleIOBoundedCrudStorage(),
            SimpleIOBoundedShardedCrudStorage(nb_shards=10),
        )

        if title == "mono_thread":
            dic = dic_mono
        else:
            dic = dic_mult

        for storage_ in storages:
            # print(type(storage_).__name__)
            # Only perform write then reads.
            # print(f"Write: {benchmark_write(storage_, KEY_VALUES_WRITE)}")
            # print(f"Read: {benchmark_read(storage_, KEY_VALUES_READ)}")
            # print()

            val = []
            val.append(benchmark_write(storage_, KEY_VALUES_WRITE))
            val.append(benchmark_read(storage_, KEY_VALUES_READ))
            name = type(storage_).__name__
            dic[name] = val
        dic["Type"] = title

    # Transform dictionary to DataFrame
    mono_df = pd.DataFrame(dic_mono, index=["Write", "Read"])
    mult_df = pd.DataFrame(dic_mult, index=["Write", "Read"])
    # Concatenate teo DataFrame of result
    df = (
        pd.concat([mono_df, mult_df], axis=0)
        .reset_index()
        .rename(columns={"index": "Opt"})
    )
    df = pd.melt(df, id_vars=["Opt", "Type"])
    print(df)

    # Visualize the result
    fig = plt.subplots(figsize=(12, 5))
    ax = sns.lineplot(data=df, x="variable", y="value", hue="Opt", style="Type")
    ax.set_xlabel("Crud storage")
    ax.set_ylabel("Time(s)")
    ax.set_title("N_KEYS = 10_000")
    plt.show()     
