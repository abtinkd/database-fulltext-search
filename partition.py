import whoosh.index as index
from whoosh.reading import IndexReader
from whoosh.searching import Searcher
from whoosh import sorting
from whoosh.qparser import QueryParser
from collections import defaultdict
import metrics as mt
from math import log
import logging
import time

LOGGER = logging.getLogger()


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
        self.update_tfidfs()

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
        return self._tfidfs

    def get_total_terms(self):
        return self._total_terms

    def update_tfidfs(self, fieldname='body'):
        effective_doc_count = 1 + self._reader.doc_count() - self.doc_count()
        for t in self._tfs:
            effective_df = 1 + self._reader.doc_frequency(fieldname, t) - self._dfs[t]
            self._tfidfs[t] = (self._tfs[t] / self._total_terms) * \
                        log(effective_doc_count / effective_df)

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

    def docs_kld(self, docnums: list, fieldname: str='body'):
        dn_kld_list = {}
        ireader = self._reader
        st, i = time.time(), 0
        for dn in docnums:
            if ireader.has_vector(dn, fieldname):
                d_tfs = ireader.vector_as('frequency', dn, fieldname)
                doc_tfs = defaultdict(int, {t:f for (t,f) in d_tfs})
                doc_tot_terms = sum(doc_tfs.values())
                dn_kld_list[dn] = mt.kl_divergence(doc_tfs, self.get_tfs(), doc_tot_terms, self.get_total_terms())
                i += 1
            else:
                LOGGER.warning('Manually assigned kld=0 for {}'.format(dn))
                dn_kld_list[dn] = 0

            if i % 1000 == 0:
                et = time.time()
                LOGGER.info('{}. kld calculation rate: {:.4f}'.format(i, (et - st) / 1000))
                st = et
        return dn_kld_list

    def doc_avg_kld(self, docnums: list, fieldname='body'):
        dn_avg_kld_list = {}
        st, i = time.time(), 0
        for dn in docnums:
            doc_tfidfs = get_doc_tfidf(self._reader, dn, fieldname)
            if doc_tfidfs != -1:
                dn_avg_kld_list[dn] = mt.avg_kl_divergence(doc_tfidfs, self.get_tfidfs())
                i += 1
            else:
                LOGGER.warning('Manually assigned avg-kld=0 for {}'.format(dn))
                dn_avg_kld_list[dn] = 0

            if i % 1000 == 0:
                et = time.time()
                LOGGER.info('{}. avg-kld calculation rate: {:.4f}'.format(i, (et - st) / 1000))
                st = et
        return dn_avg_kld_list


def get_doc_tfidf(ireader, dn, fieldname):
    if ireader.has_vector(dn, fieldname):
        doc_tfidfs = defaultdict(int)
        tot_docs = ireader.doc_count()
        d_tfs = ireader.vector_as('frequency', dn, fieldname)
        doc_tot_terms = 0
        for _, f in d_tfs:
            doc_tot_terms += f
        for t, f in d_tfs:
            doc_tfidfs[t] = (f / doc_tot_terms) * log(tot_docs / ireader.doc_frequency(fieldname, t))
        return doc_tfidfs
    else:
        return -1


def kl_divergence(part1: IndexVirtualPartition, part2: IndexVirtualPartition):
    p1_tfs = part1.get_tfs()
    p1_tts = part1.get_total_terms()
    p2_tfs = part2.get_tfs()
    p2_tts = part2.get_total_terms()
    return mt.kl_divergence(p1_tfs, p2_tfs, p1_tts, p2_tts)


def avg_kl_divergence(part1: IndexVirtualPartition, part2: IndexVirtualPartition):
    return mt.avg_kl_divergence(part1.get_tfidfs(), part2.get_tfidfs())


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


# def partition_popularity_based(index_path, low_pop_ratio, high_pop_ratio=1.0):
#     ix = index.open_dir(index_path, readonly=True)
#     facet = sorting.FieldFacet('count', reverse=True)
#
#     with ix.reader() as reader:
#         tot_docs_count = ix.doc_count()
#         cache_docs_count = int(topFraction * tot_docs_count)
#         id_list = get_sorted_ids(reader)
#         cache_id_list = id_list[:cache_docs_count]
#         db_id_list = id_list[cache_docs_count:]
#         print(reader.doc_field_length(5266, 'body'))
#         # v = reader.vector(5266, 'body')
#         # print(v)
#
#
#
#         # print(reader.frequency('body', ''))
#         # terms = reader.field_terms('body')
#     with ix.searcher() as searcher:
#         qp = MultifieldParser(['body', 'count'], schema=ix.schema)
#         qp.add_plugin(GtLtPlugin())
#         query = qp.parse('Iran')
#         results = searcher.search(query, sortedby=facet, limit=None)
#
#         for res in results:
#             print(res)

