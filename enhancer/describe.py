from partition import IndexVirtualPartition
from collections import defaultdict
import operator
from time import time, strftime
import logging
import config

LOGGER = logging.getLogger()


class PartitionDescriptor(object):

    def __init__(self, this_partition: IndexVirtualPartition, cross_partition: IndexVirtualPartition):
        self._partition = this_partition
        self._cross_partition = cross_partition
        self._ixreader = this_partition._reader
        self._ixsearcher = this_partition._searcher
        self.name = this_partition.name + '_vs_' + cross_partition.name
        self.pop_distrib = None
        self.kld_distrib = None
        self.cross_kld_distrib = None
        self.update()

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

        LOGGER.debug('{} popularity distribution is updated. [{:.4f}s]'.format(self.name, time()-st))

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
            self.kld_distrib = self._partition.doc_avg_kld(self._partition.get_docnums(), 'body')
        else:
            self.cross_kld_distrib = self._cross_partition.doc_avg_kld(self._partition.get_docnums(), 'body')

        LOGGER.debug('{} kld distribution is updated. [{:.4f}s]'.format(self.name, time()-st))

    def save(self, file_path=None):
        if file_path is None:
            file_path = 'data/{}_{}.csv'.format(strftime('%m%d_%H%M'), self.name)
        pop_distrib = self.get_sorted('pop')
        with open(file_path, 'w') as w:
            w.write(self.name +
                    ' :: aritcleId, popularity, kld_cross-kld, kld, kld_cross, docnum, count, xpath\n')
            for docnum, pop in pop_distrib:
                sf = self._ixreader.stored_fields(docnum)
                w.write('{},{},{},{},{},{},{},{}\n'
                        .format(sf['articleID'],
                                pop,
                                self.cross_kld_distrib[docnum]-self.kld_distrib[docnum],
                                self.kld_distrib[docnum],
                                self.cross_kld_distrib[docnum],
                                docnum,
                                sf['count'],
                                sf['xpath']))

