import partition as pt
from enhancer.describe import PartitionDescriptor


def naive(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):
    c = PartitionDescriptor(cache, disk)
    d = PartitionDescriptor(disk, cache)
    # print(pt.kl_divergence(cache, disk))
    c.save()
    d.save()
