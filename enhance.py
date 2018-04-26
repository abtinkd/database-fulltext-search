import config
import partition as pt
from enhancer.naive import naive
import logging

if __name__ == '__main__':
    configuration = config.get()
    pa = pt.Partitioner(configuration['wiki13_index'])
    logging.info('Partitioner is initiated!')
    parts = pa.generate([0.9])
    logging.info('Parts created!')
    parts = [p for p in parts]
    parts[0].name = 'cache'
    parts[1].name = 'disk'
    naive(parts[0], parts[1])
    # partition_popularity_based(configuration['wiki13_index'])
    # ix = index.open_dir(configuration['wiki13_index'], readonly=True)
    # whole_db = IndexVirtualPartition(ix)
    # cache_partition = IndexVirtualPartition(ix, [0], 'cache')
    # db_partition = IndexVirtualPartition(ix, [2, 3, 0], 'rest')
    # print(cache_partition.docs_kld([0]))
    # input()
    # print(cache_partition._tfs)
    # print(db_partition._tfs)
    # cache_partition.remove_doc(1)
    # db_partition.add_doc(1)
    # print(cache_partition._tfs)
    # print(db_partition._tfs)
    # print(db_partition._all_stored_fields())

