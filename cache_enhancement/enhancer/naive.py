import partition as pt
from enhancer.describe import PartitionDescriptor


def naive(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):
    inter_part_kl = pt.divergence(cache, disk)
    print(inter_part_kl)
    c = PartitionDescriptor(cache, disk)
    d = PartitionDescriptor(disk, cache)
    c.save()
    d.save()
