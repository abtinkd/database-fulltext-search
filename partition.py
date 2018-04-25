import whoosh.index as index
from whoosh import sorting
from whoosh.qparser import QueryParser, MultifieldParser, GtLtPlugin
from collections import defaultdict
import config
import metrics
from math import log
import operator
from typing import Callable


# A filter over Index
class IndexPartition(object):

    def __init__(self, file_index: index.FileIndex, index_docnums: list=None, name: str='DB'):
        self.name = name
        self.ix = file_index
        if index_docnums is not None:
            self._docnums = set(index_docnums)
        else:
            self._docnums = self._get_all_db_ids()
        self._tfs = self._get_terms('body')

    def _get_all_db_ids(self):
        all_ids = set()
        with self.ix.reader() as ireader:
            for i in ireader.all_doc_ids():
                all_ids.add(i)
        return all_ids

    def doc_count(self):
        return len(self._docnums)

    def _get_terms(self, fieldname='body'):
        with self.ix.reader() as ireader:
            if not ireader.has_vector(list(self._docnums)[0], fieldname):
                raise NotImplementedError('Forward index (vector) is not available for {}'.format(fieldname))

            tfs = defaultdict(int)
            for dn in list(self._docnums):
                tfs_list = ireader.vector_as('frequency', dn, fieldname)
                for tf in tfs_list:
                    tfs[tf[0]] += tf[1]
        return tfs

    def all_terms_count(self):
        count = 0
        for t, f in self._tfs:
            count += f
        return count

    def add_doc(self, docnum, fieldname='body'):
        if docnum not in self._docnums:
            self._docnums.add(docnum)
            with self.ix.reader() as ireader:
                if not ireader.has_vector(docnum, fieldname):
                    raise NotImplementedError('Forward index (vector) is not available for {}'.format(fieldname))

                tfs_list = ireader.vector_as('frequency', docnum, fieldname)
                for tf in tfs_list:
                    self._tfs[tf[0]] += tf[1]

    def search(self, text: str, sorted_by_count=False, fieldname='body'):
        """returns (dset, item_gen)
        results.docs(): a set of docnum of the results in index
        results.items(): a generator for (docnum, score) in from highest score/count to lowest
        """
        with self.ix.searcher() as isearcher:
            skw = {'limit': None}
            skw['q'] = QueryParser("body", self.ix.schema).parse(text)
            if sorted_by_count:
                skw['sortedby'] = sorting.FieldFacet('count', reverse=True)
            results = isearcher.search(**skw)
        return results.docs(), results.items()

    def get_tfs(self):
        return self._tfs.copy()

    def get_dfs(self):
        dfs = defaultdict(int)
        for t in self._tfs.keys():
            docnums, _ = self.search(t)
            dfs[t] = len(docnums.intersection(self._docnums))
        return dfs


    def get_tfidfs(self, fieldname='body'):
        tfidfs = defaultdict(float)
        dfs = self.get_dfs()
        with self.ix.reader() as ireader:
            for t in self._tfs.keys():
                # divide by dfs[t] as of normalization
                tfidfs[t] = (self._tfs[t]/dfs[t]) * log(ireader.doc_count() / ireader.doc_frequency(fieldname, t))
        return tfidfs

    def get_docnums(self):
        return list(self._docnums)

    def remove_doc(self, docnum, fieldname='body'):
        if docnum in self._docnums:
            self._docnums.remove(docnum)
            with self.ix.reader() as ireader:
                if not ireader.has_vector(docnum, fieldname):
                    raise NotImplementedError('Forward index (vector) is not available for {}'.format(fieldname))

                tfs_list = ireader.vector_as('frequency', docnum, fieldname)
                for tf in tfs_list:
                    self._tfs[tf[0]] -= tf[1]
                    if self._tfs[tf[0]] <= 0:
                        if self._tfs[tf[0]] == 0:
                            self._tfs.pop(tf[0])
                        else:
                            raise ValueError('Negative value for tf in partition {}'.format(self.name))

    def all_stored_fields(self):
        """ A generator for stored fields
        :return:
        {'articleID': '1', 'count': 10, 'title': 'Hi', 'xpath': '/Volumes/archive/Code/PycharmProjects/database-capacity/database-capacity/python/cache_enhancement/data/sample/sample_docs/1.txt'}
        """
        with self.ix.reader() as ireader:
            for dn in list(self._docnums):
                yield ireader.stored_fields(dn)

    def _all_stored_fields(self):
        sf = {}
        with self.ix.reader() as ireader:
            for dn in list(self._docnums):
                sf[dn] = ireader.stored_fields(dn)
        return sf

    def get_popularity_distribution(self)-> list:
        pop_dist = defaultdict(int)
        with self.ix.reader() as ireader:
            for dn in list(self._docnums):
                pop_dist[dn] = ireader.stored_fields(dn)['count']
        return sorted(pop_dist.items(), key=operator.itemgetter(1), reverse=True)

    def docs_kld(self, docnums: list, fieldname: str='body'):
        docnums = list(set(docnums))
        dn_kld_list = {}
        with self.ix.reader() as ireader:
            if not ireader.has_vector(docnums[0], fieldname):
                raise NotImplementedError('Forward index (vector) is not available for field {}'
                                          .format(fieldname))

            for dn in docnums:
                d_tfs = ireader.vector_as('frequency', dn, fieldname)
                doc_tfs = defaultdict(int, {t:f for (t,f) in d_tfs})
                dn_kld_list[dn] = distance(doc_tfs, self.get_tfs(), metrics.kl_divergence)
        return dn_kld_list

    def doc_avg_kld(self, docnum, fieldname='body'):
        with self.ix.reader() as ireader:
            if not ireader.has_vector(docnum, fieldname):
                raise NotImplementedError('Forward index (vector) is not available for doc {} field {}'
                                          .format(docnum, fieldname))

            doc_tfs = ireader.vector_as('frequency', docnum, fieldname)
            ireader.stored_fields()
        raise NotImplementedError



def distance(part1: defaultdict, part2: defaultdict, metric: Callable) -> float:
    return metric(part1, part2)


def kl_divergence(part1: IndexPartition, part2: IndexPartition):
    return distance(part1.get_tfs(), part2.get_tfs(), metrics.kl_divergence)


def avg_kl_divergence(part1: IndexPartition, part2: IndexPartition):
    return distance(part1.get_tfidfs(), part2.get_tfidfs(), metrics.avg_kl_divergence)


def combine(part1: IndexPartition, part2: IndexPartition):
    com_part = IndexPartition(part1.ix, part1.get_docnums())
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

    def get_partition_popularity(self, threasholds=[0.9]):
        threasholds += [0.0]
        threasholds = list(set(threasholds))
        threasholds.sort(reverse=True)
        tot = len(self._pop_dn)
        c = 0
        ti = 0
        docnums = []
        for pdn in self._pop_dn:
            c += 1
            docnums += [pdn[1]]
            if c > (1.0 - threasholds[ti]) * tot:
                part = IndexPartition(self._ix, docnums, 'part'+str(ti))
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


if __name__ == '__main__':
    configuration = config.get()
    pa = Partitioner(configuration['wiki13_index'])
    k = pa.get_partition_popularity([0.9])
    for kk in k:
        print(kk.name)
        for f in kk.all_stored_fields():
            print(f)
    input()
    # partition_popularity_based(configuration['wiki13_index'])
    ix = index.open_dir(configuration['wiki13_index'], readonly=True)
    whole_db = IndexPartition(ix)
    cache_partition = IndexPartition(ix, [0], 'cache')
    db_partition = IndexPartition(ix, [2, 3, 1], 'rest')
    print(cache_partition.docs_kld([2,3,1]))
    input()
    print(cache_partition._tfs)
    print(db_partition._tfs)
    cache_partition.remove_doc(1)
    db_partition.add_doc(1)
    print(cache_partition._tfs)
    print(db_partition._tfs)
    print(db_partition._all_stored_fields())