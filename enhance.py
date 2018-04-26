import partition as pt


class PartitionHolder(object):

    def __init__(self, cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):
        self.cache = cache
        self.disk = disk


def naive(cache: pt.IndexVirtualPartition, disk: pt.IndexVirtualPartition):
    cache_disk_kld = pt.kl_divergence(cache, disk)
    cdocs_cache_kld = cache.docs_kld(cache.get_docnums())
    cdocs_disk_kld = disk.docs_kld(cache.get_docnums())
    ddocs_cache_kld = cache.docs_kld(disk.get_docnums())
    ddocs_disk_kld = disk.docs_kld(disk.get_docnums())
    print(cache_disk_kld)
    print(cdocs_cache_kld)
    print(ddocs_cache_kld)

