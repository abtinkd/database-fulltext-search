from partition import IndexVirtualPartition
from collections import defaultdict
import operator
from time import time, strftime
import logging
import config

LOGGER = logging.getLogger()


class PartitionDescriptor(object):

    def __init__(self, this_partition: IndexVirtualPartition, cross_partition: IndexVirtualPartition,
                 scoring_type='tf', similarity_measure_type='avg-kld'):
        self._partition = this_partition
        self._cross_partition = cross_partition
        self.scoring_type = scoring_type
        self.similarity_measure = similarity_measure_type
        self._ixreader = this_partition._reader
        self._ixsearcher = this_partition._searcher
        self.name = this_partition.name + '_vs_' + cross_partition.name
        self.pop_distribution = None
        self.divergence_distribution = None
        self.cross_divergence_distribution = None
        self.update()

    def update(self, mode='all'):
        if mode == 'all' or 'pop':
            self._update_popularity_distribution()
        if mode == 'all' or 'div':
            self._update_divergence_distribution(cross=False)
        if mode == 'all' or 'cross-div':
            self._update_divergence_distribution(cross=True)

    def _update_popularity_distribution(self):
        LOGGER.info('{}\'s popularity distribution is being updated...'.format(self.name))
        st = time()
        pop_dist = defaultdict(int)
        for dn in self._partition.get_docnums():
            pop_dist[dn] = self._ixreader.stored_fields(dn)['count']
        self.pop_distribution = pop_dist

        LOGGER.info('{}\'s popularity distribution is updated. [{:.4f}s]'.format(self.name, time()-st))

    def get_sorted(self, mode='div', reverse=True):
        if mode == 'pop':
            d = self.pop_distribution
        if mode == 'div':
            d = self.divergence_distribution
        if mode == 'cross-div':
            d = self.cross_divergence_distribution
        return sorted(d.items(), key=operator.itemgetter(1), reverse=reverse)

    def _update_divergence_distribution(self, cross=False):
        LOGGER.info('{}\'s {} {} distribution is being updated...'
                    .format(self.name, self.scoring_type, self.similarity_measure))
        st = time()
        if not cross:
            self.divergence_distribution = \
                self._partition.docs_divergence(self._partition.get_docnums(),
                                                self.similarity_measure,
                                                self.scoring_type,
                                                'body')
        else:
            self.cross_divergence_distribution = \
                self._cross_partition.docs_divergence(self._partition.get_docnums(),
                self.similarity_measure, self.scoring_type, 'body')

        LOGGER.info('{}\'s {} {} distribution is updated. [{:.4f}s]'
                     .format(self.name, self.scoring_type, self.similarity_measure, time()-st))

    def save(self, file_path=None):
        if file_path is None:
            file_path = 'data/{}_{}.csv'.format(strftime('%m%d_%H%M'), self.name)
        pop_distrib = self.get_sorted('pop')
        with open(file_path, 'w') as w:
            w.write(self.name +
                    ' :: aritcleId, popularity, divcross-div, divcross, div, docnum, count, xpath\n')
            for docnum, pop in pop_distrib:
                sf = self._ixreader.stored_fields(docnum)
                w.write('{},{},{},{},{},{},{},{}\n'
                        .format(sf['articleID'],
                                pop,
                                self.cross_divergence_distribution[docnum] - self.divergence_distribution[docnum],
                                self.cross_divergence_distribution[docnum],
                                self.divergence_distribution[docnum],
                                docnum,
                                sf['count'],
                                sf['xpath']))
