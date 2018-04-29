import partition as pt
from enhancer.describe import PartitionDescriptor


def naive(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):
    inter_part_kl = pt.kl_divergence(cache, disk)
    print(cache.get_total_terms()/disk.get_total_terms(),
          inter_part_kl)
    print(cache.get_total_terms(), cache.doc_count())
    c = PartitionDescriptor(cache, disk)
    d = PartitionDescriptor(disk, cache)
    c.save()
    d.save()
