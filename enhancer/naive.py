import partition as pt
from enhancer.describe import PartitionDescriptor

def naive(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):
    print(pt.kl_divergence(cache, disk))
    c = PartitionDescriptor(cache, disk)
    d = PartitionDescriptor(disk, cache)
    print('cache pop', c.get_sorted('pop'))
    print('cache kld', c.get_sorted('kld'))
    print('cache ckld', c.get_sorted('ckld'))
    print('disk pop', d.get_sorted('pop'))
    print('disk kld', d.get_sorted('kld'))
    print('disk ckld', d.get_sorted('ckld'))