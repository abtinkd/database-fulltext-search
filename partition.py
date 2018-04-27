import whoosh.index as index
from whoosh import sorting
from whoosh.qparser import QueryParser
from collections import defaultdict
import metrics as mt
from math import log
from typing import Callable
import logging
import time


# A filter over Index
class IndexVirtualPartition(object):

    def __init__(self, file_index: index.FileIndex, index_docnums: list=None, name: str='DB'):
        self.name = name
        self.ix = file_index
        self._reader = file_index.reader()
        self._searcher = file_index.searcher()
        if index_docnums is not None:
            self._docnums = set(index_docnums)
        else:
            self._docnums = self._get_all_db_ids()
        self._tfs, self._total_terms = self._get_terms('body')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._reader.close()
        self._searcher.close()

    def _get_all_db_ids(self):
        all_ids = set()
        for i in self._reader.all_doc_ids():
            all_ids.add(i)
        return all_ids

    def doc_count(self):
        return len(self._docnums)

    def _get_terms(self, fieldname='body'):
        tfs = defaultdict(int)
        total_terms = 0
        for dn in list(self._docnums):
            if self._reader.has_vector(dn, fieldname):
                tfs_list = self._reader.vector_as('frequency', dn, fieldname)
                for tf in tfs_list:
                    tfs[tf[0]] += tf[1]
                    total_terms += tf[1]
            else:
                logging.warning('No forward index (vector) on {} for {}'
                                .format(fieldname, self._reader.stored_fields(dn)))
        return tfs, total_terms

    def all_terms_count(self):
        count = 0
        for t, f in self._tfs:
            count += f
        return count

    def add_doc(self, docnum, fieldname='body'):
        if docnum not in self._docnums:
            self._docnums.add(docnum)
            ireader = self._reader
            if ireader.has_vector(docnum, fieldname):
                tfs_list = ireader.vector_as('frequency', docnum, fieldname)
                for tf in tfs_list:
                    self._tfs[tf[0]] += tf[1]
            else:
                logging.warning('No forward index (vector) on {} for {}'
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
        dfs = defaultdict(int)
        for t in self._tfs.keys():
            docnums, _ = self.search(t)
            dfs[t] = len(docnums.intersection(self._docnums))
        return dfs

    def get_total_terms(self):
        return self._total_terms

    def get_tfidfs(self, fieldname='body'):
        tfidfs = defaultdict(float)
        dfs = self.get_dfs()
        ireader = self._reader
        for t in self._tfs.keys():
            # divide by dfs[t] as of normalization
            tfidfs[t] = (self._tfs[t]/dfs[t]) * log(ireader.doc_count() / ireader.doc_frequency(fieldname, t))
        return tfidfs

    def get_docnums(self):
        return list(self._docnums)

    def remove_doc(self, docnum, fieldname='body'):
        if docnum in self._docnums:
            self._docnums.remove(docnum)
            ireader = self._reader
            if ireader.has_vector(docnum, fieldname):
                tfs_list = ireader.vector_as('frequency', docnum, fieldname)
                for tf in tfs_list:
                    self._tfs[tf[0]] -= tf[1]
                    if self._tfs[tf[0]] <= 0:
                        if self._tfs[tf[0]] == 0:
                            self._tfs.pop(tf[0])
                        else:
                            raise ValueError('Negative value for tf in partition {}'.format(self.name))
            else:
                logging.warning('No forward index (vector) on {} for {}'
                                .format(fieldname, self._reader.stored_fields(docnum)))


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
                dn_kld_list[dn] = mt.kl_divergence(doc_tfs, self.get_tfs(), len(self._tfs), doc_tot_terms,
                                                   self.get_total_terms())
                i += 1
            else:
                logging.warning('Manually assigned kld=0 for {}'.format(dn))
                dn_kld_list[dn] = 0

            if i % 1000 == 0:
                et = time.time()
                print('{}. kld calculation rate: {:.4f}'.format(i, (et - st) / 1000))
                st = et
        return dn_kld_list

    def doc_avg_kld(self, docnum, fieldname='body'):
        ireader = self._reader
        if not ireader.has_vector(docnum, fieldname):
            raise NotImplementedError('Forward index (vector) is not available for doc {} field {}'
                                      .format(docnum, fieldname))
        doc_tfs = ireader.vector_as('frequency', docnum, fieldname)
        ireader.stored_fields()
        raise NotImplementedError


def kl_divergence(part1: IndexVirtualPartition, part2: IndexVirtualPartition, vocab_size: int=None):
    p1_tfs = part1.get_tfs()
    p1_tts = part1.get_total_terms()
    p2_tfs = part2.get_tfs()
    p2_tts = part2.get_total_terms()
    if vocab_size is None:
        vocab_size = len(p1_tfs) if len(p1_tfs) >= len(p2_tfs) else len(p2_tfs)
    return mt.kl_divergence(p1_tfs, p2_tfs, vocab_size, p1_tts, p2_tts)


def avg_kl_divergence(part1: IndexVirtualPartition, part2: IndexVirtualPartition, vocabularies: set=None):
    if vocabularies is None:
        vocabularies = set(part1.get_tfs().keys())
        vocabularies.update(part2.get_tfs().keys())
    return mt.avg_kl_divergence(part1.get_tfidfs(), part2.get_tfidfs(), vocabularies)


def combine(part1: IndexVirtualPartition, part2: IndexVirtualPartition):
    com_part = IndexVirtualPartition(part1.ix, part1.get_docnums())
    for dn in part2.get_docnums():
        com_part.add_doc(dn)
    return com_part


class Partitioner(object):

    def __init__(self, index_path: str):
        self._ix = index.open_dir(index_path, readonly=True)
        self._pop_dn = []
        with self._ix.reader() as ireader:
            for dx in ireader.iter_docs():
                self._pop_dn += [(int(dx[1]['count']), dx[0])]
        self._pop_dn.sort(reverse=True)

    def generate(self, threasholds=[0.9]):
        threasholds += [0.0, 1.0]
        threasholds = list(set(threasholds))
        threasholds.sort(reverse=True)
        c = 0
        ti = 1
        docnums = []
        for pdn in self._pop_dn:
            c += 1
            docnums += [pdn[1]]
            if c >= (1.0 - threasholds[ti]) * len(self._pop_dn):
                part = IndexVirtualPartition(self._ix, docnums,
                                             '{}-{}partition'.format(threasholds[ti], threasholds[ti-1]))
                docnums = []
                ti += 1
                yield part


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

