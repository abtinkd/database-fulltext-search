import whoosh.index as index
from whoosh import sorting
from whoosh.qparser import QueryParser, MultifieldParser, GtLtPlugin
from collections import defaultdict
import config


# A filter over Index
class IndexPartition(object):

    def __init__(self, file_index: index.FileIndex, index_docnums: set, name: str):
        self.name = name
        self.ix = file_index
        self.docnums = index_docnums
        self._tfs = self._get_terms('body')
        self.ix.reader()

    def doc_count(self):
        return len(self.docnums)

    def _get_terms(self, fieldname='body'):
        with self.ix.reader() as ireader:
            if not ireader.has_vector(list(self.docnums)[0], fieldname):
                raise NotImplementedError('Forward index (vector) is not available for {}'.format(fieldname))

            tfs = defaultdict(int)
            for dn in list(self.docnums):
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
        if docnum not in self.docnums:
            self.docnums.add(docnum)
            with self.ix.reader() as ireader:
                if not ireader.has_vector(docnum, fieldname):
                    raise NotImplementedError('Forward index (vector) is not available for {}'.format(fieldname))

                tfs_list = ireader.vector_as('frequency', docnum, fieldname)
                for tf in tfs_list:
                    self._tfs[tf[0]] += tf[1]

    def get_tfs(self):
        return self._tfs.copy()

    def remove_doc(self, docnum, fieldname='body'):
        if docnum in self.docnums:
            self.docnums.remove(docnum)
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
        with self.ix.reader() as ireader:
            for dn in list(self.docnums):
                yield ireader.stored_fields(dn)

    def _all_stored_fields(self):
        sf = {}
        with self.ix.reader() as ireader:
            for dn in list(self.docnums):
                sf[dn] = ireader.stored_fields(dn)
        return sf


def avg_kl_divergence(part1: IndexPartition, part2: IndexPartition, fieldname='body') -> float:
    part1_tfs = part1.get_tfs()
    part2_tfs = part2.get_tfs()
    combined_tfs = defaultdict(int)
    for t, f in part1_tfs.items():
        combined_tfs[t] += f
    for t, f in part2_tfs.items():
        combined_tfs[t] += f

    sum1 = 0
    for dfs in part1.all_stored_fields():
        # for t in dfs[fieldname]:
        pass

def get_sorted_ids(index_reader):
    count_id = []
    for doc_ix in index_reader.iter_docs():
        count_id += [(doc_ix[1]['count'], doc_ix[1]['articleID'], doc_ix[0])]
    count_id = sorted(count_id, reverse=True)
    return count_id


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
    # partition_popularity_based(configuration['wiki13_index'])
    ix = index.open_dir(configuration['wiki13_index'])
    cache_partition = IndexPartition(ix, set([0, 1]), 'cache')
    db_partition = IndexPartition(ix, set([2, 3]), 'db')
    print(cache_partition._tfs)
    print(db_partition._tfs)
    cache_partition.remove_doc(1)
    db_partition.add_doc(1)
    print(cache_partition._tfs)
    print(db_partition._tfs)
    print(db_partition._all_stored_fields())