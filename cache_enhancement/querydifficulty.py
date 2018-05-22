import pandas as pd
import sys
from collections import defaultdict

from whoosh.qparser import QueryParser

import config
from partition import IndexVirtualPartition, Partitioner
import whoosh.index as index
import whoosh.analysis as analysis
from math import log
from whoosh.reading import MultiReader
from whoosh.searching import Searcher
import logging
from functools import reduce

LOGGER = logging.getLogger()


def tokenize(query: str) -> defaultdict:
    analyzer = analysis.StandardAnalyzer()
    tokens = defaultdict(int)
    for token in analyzer(query):
        tokens[token.text] += 1
    return tokens


def get_index_docnum_of_article_id(article_id: str, ix_reader: MultiReader, article_id_fieldname='articleID'):
    ix_searcher = Searcher(ix_reader)
    rslts = ix_searcher.search(QueryParser(article_id_fieldname, ix_reader.schema).parse(article_id))
    if len(rslts) == 0:
        LOGGER.warning('Article ID {} was not found in the index'.format(article_id))
        return -1
    if len(rslts) > 1:
        LOGGER.warning('Article ID {} has multiple instances in the index'.format(article_id))
    return rslts[0].docnum


def get_docs_tfs(article_ids: list, ix_reader: MultiReader, fieldname='body') -> defaultdict(lambda: defaultdict(int)):
    docs_tfs = defaultdict(lambda: defaultdict(int))
    for aId in article_ids:
        dn = get_index_docnum_of_article_id(aId, ix_reader)
        if dn == -1:
            continue
        if ix_reader.has_vector(dn, fieldname):
            tf_d = dict(ix_reader.vector_as('frequency', dn, fieldname))
            docs_tfs[aId] = tf_d
        else:
            LOGGER.warning('No forward vector was found for docnum {}, articleID {}'.format(dn, aId))
    return docs_tfs


def specificity(query: str, tf_collection: defaultdict, total_collection_terms: int) -> float:
    tf_query = tokenize(query)
    total_query_terms = sum(tf_query.values())
    scs = 0.0
    for term, frequency in tf_query.items():
        prob_t_conditioned_query = frequency / total_query_terms
        prob_t_conditioned_collection = tf_collection[term] / total_collection_terms
        scs += prob_t_conditioned_query * log(prob_t_conditioned_query / prob_t_conditioned_collection)
    return scs


def similarity(query: str, collection: IndexVirtualPartition, mode='avg') -> float:
    # mode is in {'avg', 'max', 'sum'}
    query_terms = list(tokenize(query).keys())
    idfs = collection.get_tfidfs()
    tfs = collection.get_tfs()
    if mode == 'avg':
        scq = 0.0
        for term in query_terms:
            scq += (1 + log(tfs[term])) * idfs[term]
        return scq / len(query_terms)
    if mode == 'sum':
        scq = 0.0
        for term in query_terms:
            scq += (1 + log(tfs[term])) * idfs[term]
        return scq
    if mode == 'max':
        scq = [(1 + log(tfs[term])) * idfs[term] for term in query_terms]
        return max(scq)


def clarity(query: str, query_result_docs_tfs: defaultdict(lambda: defaultdict(int)),
            collection: IndexVirtualPartition) -> float:
    query_terms = list(tokenize(query).keys())
    collection_tfs = collection.get_tfs()
    collection_total_terms = collection.get_total_terms()
    vocabulary = list(collection_tfs.keys())

    def get_prob_t_condition_Dq(t: str, lambd=0.9) -> float:
        prob_t_condit_D = collection_tfs[t] / collection_total_terms
        norm = 0.0
        for tf_d in query_result_docs_tfs.values():
            tot_d = sum(tf_d.values())
            norm += reduce(lambda tm, y: (tf_d[tm]/tot_d)*y, query_terms)
        prob = 0.0
        for dn, tf_d in query_result_docs_tfs.items():
            tot_d = sum(tf_d.values())
            prob_t_condit_d = lambd * (tf_d[t]/tot_d) + (1-lambd) * prob_t_condit_D
            prob_q_condit_d = reduce(lambda tm, y: (tf_d[tm]/tot_d)*y, query_terms)
            prob_d_condit_q = prob_q_condit_d / norm
            prob += prob_t_condit_d * prob_d_condit_q
        return prob

    clt = 0.0
    for t in vocabulary:
        prob_t_condit_D = collection_tfs[t] / collection_total_terms
        prob_t_condit_Dq = get_prob_t_condition_Dq(t)
        clt += prob_t_condit_Dq * log(prob_t_condit_Dq / prob_t_condit_D)
    return clt


if __name__ == '__main__':
    index_name = sys.argv[1]
    query_file_path = sys.argv[2]
    save_path = sys.argv[3]

    c = config.get_paths()
    ix = index.open_dir(c[index_name], readonly=True)
    LOGGER.info('Index path: ' + c[index_name])
    ix_reader = ix.reader()

    database = IndexVirtualPartition(ix, None, 'DB', ix_reader, 'body')

    df = pd.read_csv(query_file_path)
    uniqueries = df['query'].unique()
    for q in uniqueries:
        qdf = df[df['query'] == q]
        scs = specificity(q, database.get_tfs(), database.get_total_terms())

        aids = list(qdf['articleId'])
        docs_tfs = get_docs_tfs(aids, ix_reader)
        clt = clarity(q, docs_tfs, database)

        df.loc[qdf.index, 'specificity'] = scs
        df.loc[qdf.index, 'clarity'] = clt

    ix_reader.close()
    df.to_csv(save_path, index=False)
