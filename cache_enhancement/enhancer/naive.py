import partition as pt
from enhancer.describe import PartitionDescriptor


def naive(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):
    print('kld divergence({}, {}) = {}'.format(cache.name, disk.name, pt.divergence(cache, disk)))
    des = PartitionDescriptor(cache, disk, similarity_measure_type='kld', update_modes=['pop', 'cross-div'])
    print('saving ...')
    des.save()
    print('avg-kld divergence({}, {}) = {}'.format(cache.name, disk.name, pt.divergence(cache, disk)))
    des = PartitionDescriptor(cache, disk, similarity_measure_type='avg-kld', update_modes=['pop', 'cross-div'])
    des.save()
    print('saving ...')

