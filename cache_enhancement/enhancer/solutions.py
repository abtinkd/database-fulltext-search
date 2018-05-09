import partition as pt
from enhancer.describe import PartitionDescriptor
import logging
import pandas as pd
import sys

LOGGER = logging.getLogger()


def load_distribution_csv(file_path: str, start_range: float=0.0, end_range: float=1.0) -> pd.DataFrame:
    lines = 0
    with open(file_path, 'r') as fo:
        for l in fo:
            lines += 1

    skip_range = range(1, int(start_range * lines))  # keeps first row for column names
    nbr_rows_to_read = int((end_range-start_range) * lines)
    distribution_df = pd.read_csv(file_path, sep=',', nrows=nbr_rows_to_read, skiprows=skip_range)

    first_column_name = distribution_df.columns[0]
    parts = first_column_name.split('::')
    distribution_df.rename(columns={first_column_name: parts[0]})
    distribution_df.columns.name = parts[1]

    return distribution_df


def generate_distance_distributions(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition,
                                    save_path: str, distance_type: list=['avg-kld']):
    def repeat(sm, sc):
        div_val = pt.divergence(cache, disk, similarity_measure_type=sm, score_type=sc)
        LOGGER.info('{} {} ivergence({}, {}) = {}'.format(sc, sm, cache.name, disk.name, div_val))
        des = PartitionDescriptor(cache, disk, similarity_measure_type=sm, update_modes=['pop', 'div', 'cross-div'])
        print('saving in {} ...'.format(save_path))
        des.save(save_path)

    for d in distance_type:
        repeat(sm=d, sc='tf')


def naive(cache_distribution_path: str, disk_distribution_path: str):
    cache_df = load_distribution_csv(cache_distribution_path, start_range=0.5)
    disk_df = load_distribution_csv(disk_distribution_path, start_range=0.0, end_range=0.05)


if __name__ == '__main__':
    df = load_distribution_csv(sys.argv[1], start_range=sys.argv[2], end_range=sys.argv[3])
    print(df.describe())
