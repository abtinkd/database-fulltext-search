import config
import partition as pt
from enhancer.naive import naive
from whoosh import index
import config
import sys


if __name__ == '__main__':
    index_name = sys.argv[1]
    config.setup_logger(index_name+'_enhance')

    configuration = config.get_paths()
    ix = index.open_dir(configuration[index_name], readonly=True)
    with ix.reader() as ix_reader:
        pa = pt.Partitioner(ix, ix_reader)
        print('Partitioner initiated!')
        parts = pa.generate([0.98, 0.90, 0.7])
        print('Parts created!')
        parts = [p for p in parts]
        naive(parts[1], parts[2])
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

