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
    distribution_df = pd.read_csv(file_path, sep=',', nrows=nbr_rows_to_read, skiprows=skip_range,
                                  skipinitialspace=True)

    first_column_name = distribution_df.columns[0]
    parts = first_column_name.split('::')
    distribution_df = distribution_df.rename(columns={first_column_name: parts[0]})
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


def recursive_refine(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition,
                     save_log_path: str, distance_type: str='kld', score_type: str='tf'):
    removed_docs_cache = []
    prev_div_val = 0.0
    step = 0
    while step < 25:
        step += 1
        LOGGER.info("round: {}, cache size: {}".format(step, cache.doc_count()))
        print("round: {}, cache size: {}".format(step, cache.doc_count()))
        div_val = pt.divergence(cache, disk, similarity_measure_type=distance_type, score_type=score_type)
        LOGGER.info('{} {} ivergence({}, {}) = {}'.format(distance_type, score_type, cache.name, disk.name, div_val))
        if div_val <= prev_div_val:
            break
        prev_div_val = div_val
        descriptor_cache_vs_disk = PartitionDescriptor(cache, disk, similarity_measure_type=distance_type,
                                                       update_modes=['pop', 'div', 'cross-div'])
        pop_distrib = descriptor_cache_vs_disk.pop_distribution
        div_distrib = descriptor_cache_vs_disk.divergence_distribution
        cross_div_distrib = descriptor_cache_vs_disk.cross_divergence_distribution

        cname = score_type+distance_type
        cache_df = pd.DataFrame({'articleId': pop_distrib, cname : div_distrib, 'cross_'+cname: cross_div_distrib})
        remove_candidates = list(cache_df[cache_df['cross_'+cname]-cache_df[cname] < 0].index)
        for dn in remove_candidates:
            removed_docs_cache.append(dn)
            cache.remove_doc(dn)

    save_log_path = save_log_path[:-1] if save_log_path[-1] == '/' else save_log_path
    fw_cache = open('{}/recur_{}_{}_cache_update_log.csv'
                    .format(save_log_path, cache.name, disk.name), 'w')
    fw_disk = open('{}/recur_{}_{}_disk_update_log.csv'
                   .format(save_log_path, cache.name, disk.name), 'w')
    for dn in removed_docs_cache:
        fw_cache.write('d, {}, {}, {}\n'.format(dn, 'void', 'void'))
        fw_disk.write('a, {}, {}, {}\n'.format(dn, 'void', 'void'))
    fw_cache.close()
    fw_disk.close()


def naive1(cache_distribution_path: str, disk_distribution_path: str, save_log_path: str, use_column_with_index: int,
           cache_start_range: float, cache_end_range: float,
           disk_start_range: float, disk_end_range: float,
           equal_add_delete: bool=True):
    cache_df = load_distribution_csv(cache_distribution_path, 
                                     start_range=cache_start_range, end_range=cache_end_range)
    disk_df = load_distribution_csv(disk_distribution_path, 
                                    start_range=disk_start_range, end_range=disk_end_range)
    #  0.0 <= cache_start_range, disk_start_range <= cache_end_range, disk_end_range <= 1.0
    
    pivot_col = cache_df.columns[use_column_with_index]

    cache_remove_df = cache_df[cache_df[pivot_col] < 0.0]
    disk_remove_df = disk_df[disk_df[pivot_col] < 0.0]
    cache_remove_df = cache_remove_df.sort_values(by=pivot_col, ascending=True)
    disk_remove_df = disk_remove_df.sort_values(by=pivot_col, ascending=True)
    LOGGER.info('Naive1, {}, eq={}, CaS={}, CaE={}, DiS={}, DiE={}, CaPath:{}, DiPath:{}'
                .format(pivot_col, equal_add_delete, cache_start_range, cache_end_range,
                        disk_start_range, disk_end_range, cache_distribution_path, disk_distribution_path))
    save_log_path = save_log_path[:-1] if save_log_path[-1] == '/' else save_log_path
    fw_cache = open('{}/niv1_{}_{}-{}_{}-{}_cache_update_log.csv'
                    .format(save_log_path, pivot_col, cache_start_range, cache_end_range, disk_start_range, disk_end_range), 'w')
    fw_disk = open('{}/niv1_{}_{}-{}_{}-{}_disk_update_log.csv'
                   .format(save_log_path, pivot_col, cache_start_range, cache_end_range, disk_start_range, disk_end_range), 'w')

    min_change = cache_remove_df.shape[0] if cache_remove_df.shape[0] < disk_remove_df.shape[0] else disk_remove_df.shape[0]
    c = 0
    for _, row in cache_remove_df.iterrows():
        c += 1
        fw_cache.write('d, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[pivot_col]))
        fw_disk.write('a, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[pivot_col]))
        if equal_add_delete and c >= min_change:
            break
    c = 0
    for _, row in disk_remove_df.iterrows():
        c += 1
        fw_cache.write('a, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[pivot_col]))
        fw_disk.write('d, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[pivot_col]))
        if equal_add_delete and c >= min_change:
            break

    fw_cache.close()
    fw_disk.close()


def naive2(cache_distribution_path: str, disk_distribution_path: str, save_log_path: str, change_fraction: float,
           cache_start_range: float, cache_end_range: float,
           disk_start_range: float, disk_end_range: float,
           equal_add_delete: bool = True):
    cache_df = load_distribution_csv(cache_distribution_path,
                                     start_range=cache_start_range, end_range=cache_end_range)
    disk_df = load_distribution_csv(disk_distribution_path,
                                    start_range=disk_start_range, end_range=disk_end_range)
    #  0.0 <= cache_start_range, disk_start_range <= cache_end_range, disk_end_range <= 1.0

    div_col = cache_df.columns[4]
    div_cross_col = cache_df.columns[3]

    # disk_remove_df = disk_df[disk_df[pivot_col] < 0.0]
    cache_df = cache_df.sort_values(by=div_col, ascending=True)
    disk_df = disk_df.sort_values(by=div_cross_col, ascending=False)
    LOGGER.info('Naive2-variety, {}, eq={}, CaS={}, CaE={}, DiS={}, DiE={}, CaPath:{}, DiPath:{}'
                .format(div_col, equal_add_delete, cache_start_range, cache_end_range,
                        disk_start_range, disk_end_range, cache_distribution_path, disk_distribution_path))
    save_log_path = save_log_path[:-1] if save_log_path[-1] == '/' else save_log_path
    fw_cache = open('{}/niv2_{}_{}-{}_{}-{}_cache_update_log.csv'
                    .format(save_log_path, div_col, cache_start_range, cache_end_range, disk_start_range,
                            disk_end_range), 'w')
    fw_disk = open('{}/niv2_{}_{}-{}_{}-{}_disk_update_log.csv'
                   .format(save_log_path, div_col, cache_start_range, cache_end_range, disk_start_range,
                           disk_end_range), 'w')

    c = 0
    for _, row in cache_df.iterrows():
        c += 1
        fw_cache.write('d, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[div_col]))
        fw_disk.write('a, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[div_cross_col]))
        if equal_add_delete and c >= int(cache_df.shape[0]*change_fraction):
            break
    c = 0
    for _, row in disk_df.iterrows():
        c += 1
        fw_cache.write('a, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[div_cross_col]))
        fw_disk.write('d, {}, {}, {}\n'.format(row['articleId'], row['xpath'], row[div_col]))
        if equal_add_delete and c >= int(cache_df.shape[0]*change_fraction):
            break

    fw_cache.close()
    fw_disk.close()
