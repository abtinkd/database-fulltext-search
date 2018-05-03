import partition as pt
from enhancer.describe import PartitionDescriptor
import logging

LOGGER = logging.getLogger()


def naive(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):

    def repeat(sm, sc):
        div_val = pt.divergence(cache, disk, similarity_measure_type=sm, score_type=sc)
        LOGGER.info('{} {} ivergence({}, {}) = {}'.format(sc, sm, cache.name, disk.name, div_val))
        des = PartitionDescriptor(cache, disk, similarity_measure_type=sm, update_modes=['pop', 'div', 'cross-div'])
        print('saving ...')
        des.save('/data/khodadaa/index/data')

    repeat(sm='kld', sc='tf')
    repeat(sm='avg-kld', sc='tf')
