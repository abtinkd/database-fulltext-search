import whoosh.index as index
from whoosh.reading import IndexReader, MultiReader
from whoosh.searching import Searcher
from whoosh import sorting
from whoosh.qparser import QueryParser
from collections import defaultdict
import metrics as mt
from math import log
import logging
import time

LOGGER = logging.getLogger()


def get_database_tfs(ix_reader: MultiReader, field_name='body'):
    LOGGER.info('Building TF for [{}] field of the Index'.format(field_name))
    tfs = defaultdict(lambda: defaultdict(int))
    all_terms = ix_reader.field_terms(field_name)
    for term in all_terms:
        f = ix_reader.frequency(field_name, term)
        tfs[term] = f
    return tfs


# A filter over Index
class IndexVirtualPartition(object):

    def __init__(self, file_index: index.FileIndex, index_docnums: list=None, name: str='DB',
                 ix_reader: IndexReader=None, content_field='body'):
        self.name = name
        self.ix = file_index
        self._reader = ix_reader if ix_reader is not None else file_index.reader()
        self._private_reader = False if ix_reader is not None else True
        self._searcher = Searcher(self._reader)
        if index_docnums is not None:
            self._docnums = set(index_docnums)
        else:
            self._docnums = self._get_all_db_ids()
        self._dfs, self._tfs, self._total_terms = self._build(content_field)
        self._tfidfs = defaultdict(float)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._private_reader:
            self._reader.close()

    def _get_all_db_ids(self):
        all_ids = set()
        for i in self._reader.all_doc_ids():
            all_ids.add(i)
        return all_ids

    def doc_count(self):
        return len(self._docnums)

    def _build(self, fieldname='body'):
        tfs, dfs = defaultdict(int), defaultdict(int)
        total_terms = 0
        for dn in list(self._docnums):
            if self._reader.has_vector(dn, fieldname):
                tfs_list = self._reader.vector_as('frequency', dn, fieldname)
                for tf in tfs_list:
                    tfs[tf[0]] += tf[1]
                    total_terms += tf[1]
                    dfs[tf[0]] += 1
            else:
                LOGGER.warning('No forward index (vector) on {} for {}'
                                .format(fieldname, self._reader.stored_fields(dn)))
        return dfs, tfs, total_terms

    def all_terms_count(self):
        return self._total_terms

    def add_doc(self, docnum, fieldname='body'):
        if docnum not in self._docnums:
            self._docnums.add(docnum)
            ireader = self._reader
            if ireader.has_vector(docnum, fieldname):
                tfs_list = ireader.vector_as('frequency', docnum, fieldname)
                for tf in tfs_list:
                    self._tfs[tf[0]] += tf[1]
                    self._total_terms += tf[1]
                    self._dfs[tf[0]] += 1
            else:
                LOGGER.warning('No forward index (vector) on {} for {}'
                                .format(fieldname, self._reader.stored_fields(docnum)))

    def remove_doc(self, docnum, fieldname='body'):
        if docnum in self._docnums:
            self._docnums.remove(docnum)
            ireader = self._reader
            if ireader.has_vector(docnum, fieldname):
                tfs_list = ireader.vector_as('frequency', docnum, fieldname)
                for tf in tfs_list:
                    self._tfs[tf[0]] -= tf[1]
                    self._total_terms -= tf[1]
                    self._dfs[tf[0]] -= 1
                    if self._tfs[tf[0]] <= 0:
                        if self._tfs[tf[0]] == 0:
                            self._tfs.pop(tf[0])
                            self._dfs.pop(tf[0])
                        else:
                            raise ValueError('Negative value for tf in partition {}'.format(self.name))
            else:
                LOGGER.warning('No forward index (vector) on {} for {}'
                                .format(fieldname, self._reader.stored_fields(docnum)))

    def search(self, text: str, sorted_by_count=False, fieldname='body'):
        """returns (dset, item_gen)
        results.docs(): a set of docnum of the results in index
        results.items(): a generator for (docnum, score) in from highest score/count to lowest
        """
        isearcher = self._searcher
        skw = {'limit': None}
        skw['q'] = QueryParser("body", self.ix.schema).parse(text)
        if sorted_by_count:
            skw['sortedby'] = sorting.FieldFacet('count', reverse=True)
        results = isearcher.search(**skw)
        return results.docs(), results.items()

    def get_tfs(self):
        # return self._tfs.copy()
        return self._tfs

    def get_dfs(self):
        # return self._dfs.copy()
        return self._dfs

    def get_tfidfs(self):
        if len(self._tfidfs) == 0:
            raise ValueError('Empty tfidf. It must be updated before use.')
        return self._tfidfs

    def get_total_terms(self):
        return self._total_terms

    def update_tfidfs(self, fieldname='body'):
        effective_doc_count = 1 + self._reader.doc_count() - self.doc_count()
        for t in self._tfs:
            effective_df = 1 + self._reader.doc_frequency(fieldname, t) - self._dfs[t]
            self._tfidfs[t] = self._tfs[t] * log(effective_doc_count / effective_df)

    def get_docnums(self):
        return list(self._docnums)

    def all_stored_fields(self):
        """ A generator for stored fields
        :return:
        {'articleID': '1', 'count': 10, 'title': 'Hi', 'xpath': '/Volumes/archive/Code/PycharmProjects/database-capacity/database-capacity/python/cache_enhancement/data/sample/sample_docs/1.txt'}
        """
        ireader = self._reader
        for dn in list(self._docnums):
            yield ireader.stored_fields(dn)

    def _all_stored_fields(self):
        sf = {}
        ireader = self._reader
        for dn in list(self._docnums):
            sf[dn] = ireader.stored_fields(dn)
        return sf

    def docs_divergence(self, docnums: list,
                        similarity_measure_type: str='avg-kld',
                        score_type: str='tf',
                        fieldname: str='body'):
        div = {}
        st = time.time()
        LOGGER.info('Calculating {} of {} documents from {} based on {}'.format(similarity_measure_type,
                                                                                len(docnums),
                                                                                self.name,
                                                                                score_type))
        term_scoring, doc_tot_terms = None, 0
        for dn in docnums:
            if score_type == 'tf':
                term_scoring, doc_tot_terms = get_doc_tf(self._reader, dn, fieldname)
            elif score_type == 'tfidf':
                term_scoring, doc_tot_terms = get_doc_tfidf(self._reader, dn, fieldname)
            if term_scoring is not None:
                if similarity_measure_type == 'avg-kld':
                    div[dn] = mt.avg_kl_divergence(term_scoring, self.get_tfs(), doc_tot_terms, self.get_total_terms())
                elif similarity_measure_type == 'kld':
                    div[dn] = mt.kl_divergence(term_scoring, self.get_tfs(), doc_tot_terms, self.get_total_terms())
            else:
                LOGGER.warning('Manually assigned {}=0.0 for {}'.format(similarity_measure_type, dn))
                div[dn] = 0
        LOGGER.info('{} {} calculation rate: {} d/s'.format(score_type, similarity_measure_type,
                                                              len(docnums)/(time.time()-st)))
        return div


def get_doc_tf(ireader, dn, fieldname):
    if ireader.has_vector(dn, fieldname):
        doc_tfs = defaultdict(int)
        doc_tot_terms = 0
        for t, f in ireader.vector_as('frequency', dn, fieldname):
            doc_tfs[t] = f
            doc_tot_terms += f
        return doc_tfs, doc_tot_terms
    else:
        return None, 0


def get_doc_tfidf(ireader, dn, fieldname):
    if ireader.has_vector(dn, fieldname):
        doc_tfidfs = defaultdict(int)
        tot_docs = ireader.doc_count()
        doc_tot_terms = 0
        for t, f in ireader.vector_as('frequency', dn, fieldname):
            doc_tfidfs[t] = f * log(tot_docs / ireader.doc_frequency(fieldname, t))
            doc_tot_terms += f
        return doc_tfidfs, doc_tot_terms
    else:
        return None, 0


def divergence(part1: IndexVirtualPartition, part2: IndexVirtualPartition,
               similarity_measure_type: str='avg-kld', score_type: str='tf', fieldname: str='body'):
    st = time.time()
    LOGGER.info('Calculating {} of {} from {} based on {}'
                .format(similarity_measure_type, part1.name, part2.name, score_type))
    div, measure1, measure2 = None, None, None
    if score_type == 'tf':
        measure1 = part1.get_tfs()
        measure2 = part2.get_tfs()
    elif score_type == 'tfidf':
        measure1 = part1.get_tfidfs()
        measure2 = part2.get_tfidfs()
    if similarity_measure_type == 'avg-kld':
        div = mt.avg_kl_divergence(measure1, measure2, part1.get_total_terms(), part2.get_total_terms())
    elif similarity_measure_type == 'kld':
        div = mt.kl_divergence(measure1, measure2, part1.get_total_terms(), part2.get_total_terms())
    LOGGER.info('{} {} Calculation time: {} min'.format(score_type, similarity_measure_type,
                                                                    (time.time() - st)/60))
    return div


def combine(part1: IndexVirtualPartition, part2: IndexVirtualPartition):
    com_part = IndexVirtualPartition(part1.ix, part1.get_docnums())
    for dn in part2.get_docnums():
        com_part.add_doc(dn)
    return com_part


class Partitioner(object):

    def __init__(self, ix: index.FileIndex, ix_reader: IndexReader):
        self._ix = ix
        self._reader = ix_reader
        self._pop_dn = []
        for dx in ix_reader.iter_docs():
            self._pop_dn += [(int(dx[1]['count']), dx[0])]
        self._pop_dn.sort(reverse=True)

    def generate(self, threasholds=[0.9]):
        threasholds += [1.0]
        threasholds = list(set(threasholds))
        threasholds.sort(reverse=True)
        c = 0
        ti = 1
        docnums = []
        for pdn in self._pop_dn:
            c += 1
            docnums += [pdn[1]]
            if c >= (1.0 - threasholds[ti]) * len(self._pop_dn):
                yield IndexVirtualPartition(self._ix, docnums,
                                            '{}-{}_part'.format(threasholds[ti], threasholds[ti-1]),
                                            self._reader)
                docnums = []
                ti += 1
                if ti == len(threasholds):
                    break
