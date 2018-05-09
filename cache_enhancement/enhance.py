import config
import partition as pt
import enhancer.solutions as sol
from whoosh import index
import config, logging
import sys

LOGGER = logging.getLogger()

if __name__ == '__main__':
    index_name = sys.argv[1]
    config.setup_logger(file_name=index_name+'_enhance')

    configuration = config.get_paths()
    ix = index.open_dir(configuration[index_name], readonly=True)
    LOGGER.info('Index path: '+configuration[index_name])
    with ix.reader() as ix_reader:
        pa = pt.Partitioner(ix, ix_reader)
        print('Partitioner initiated!')
        parts = pa.generate([0.98, 0.1])
        parts = [p for p in parts]
        print('Parts created!')
        print('naive ({}, {})'.format(parts[0].name, parts[1].name))
        sol.generate_distance_distributions(cache=parts[0], disk=parts[1],
                                            save_path='/data/khodadaa/index/data', distance_type=['kld', 'avg-kld'])
        # for dn in [10439,7634,1701,9761,6697,8430,10576,11162,4767,4610]:
        # # for dn in [1175,8765,7297,5619,2471,9536,7885,10711,6007,10814]:
        #     parts[2].add_doc(dn)
        #     parts[1].remove_doc(dn)
        # for dn in [2715,10378,4246,3517,5316,4325,11263,11973,173,597]:
        # # for dn in [8651, 5129, 7265, 10758, 5038, 3764, 1484, 2303, 10893, 6833]:
        #     parts[1].add_doc(dn)
        #     parts[2].remove_doc(dn)
        # print('new-naive(parts[1], parts[2])')
        # naive(parts[1], parts[2])
        # print('new-naive(parts[2], parts[1])')
        # naive(parts[2], parts[1])
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

