from partition import IndexVirtualPartition
from collections import defaultdict
import operator
from time import time

LOG = True


class PartitionDescriptor(object):

    def __init__(self, this_partition: IndexVirtualPartition, cross_partition: IndexVirtualPartition):
        st = time()
        self._partition = this_partition
        self._cross_partition = cross_partition
        self._ixreader = this_partition.ix.reader()
        # self._ixsearcher = this_partition.ix.searcher()
        self.name = this_partition.name + '_descriptor'
        self.pop_distrib = None
        self.kld_distrib = None
        self.cross_kld_distrib = None
        self.update()

        if LOG:
            print('{} is initialized. [{:.4f}s]'.format(self.name, time()-st))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ixreader.close()
        # self._ixsearcher.close()

    def update(self, mode='all'):
        if mode == 'all' or 'pop':
            self._update_popularity_distribution()
        if mode == 'all' or 'kld':
            self._update_kld_distribution(cross=False)
        if mode == 'all' or 'ckld':
            self._update_kld_distribution(cross=True)

    def _update_popularity_distribution(self):
        st = time()
        pop_dist = defaultdict(int)
        for dn in self._partition.get_docnums():
            pop_dist[dn] = self._ixreader.stored_fields(dn)['count']
        self.pop_distrib = pop_dist

        if LOG:
            print('{} popularity distribution is updated. [{:.4f}s]'.format(self.name, time()-st))

    def get_sorted(self, mode='kld', reverse=True):
        if mode == 'pop':
            d = self.pop_distrib
        if mode == 'kld':
            d = self.kld_distrib
        if mode == 'ckld':
            d = self.cross_kld_distrib
        return sorted(d.items(), key=operator.itemgetter(1), reverse=reverse)

    def _update_kld_distribution(self, cross=False):
        st = time()
        if not cross:
            self.kld_distrib = self._partition.docs_kld(self._partition.get_docnums(), 'body')
        else:
            self.cross_kld_distrib = self._cross_partition.docs_kld(self._partition.get_docnums(), 'body')

        if LOG:
            print('{} kld distribution is updated. [{:.4f}s]'.format(self.name, time()-st))
