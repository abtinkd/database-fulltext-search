import partition as pt
from enhancer.describe import PartitionDescriptor
import logging
import pandas as pd

LOGGER = logging.getLogger()


def load_distribution_csv(file_path: str, start_range: float=0.0, end_range: float=1.0) -> pd.DataFrame:
    lines = 0
    with open(file_path, 'r') as fo:
        for l in fo:
            lines += 1

    start_line = start_range * lines
    end_line = end_range * lines
    return pd.read_csv(file_path, sep=',', nrows=end_line-start_line, skiprows=start_line)


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





