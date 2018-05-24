import pandas as pd
import sys
from collections import defaultdict

from whoosh.qparser import QueryParser

import config
import metrics
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
            tf_d = defaultdict(int, ix_reader.vector_as('frequency', dn, fieldname))
            docs_tfs[aId] = tf_d
        else:
            LOGGER.warning('No forward vector was found for docnum {}, articleID {}'.format(dn, aId))
    return docs_tfs


def specificity(query: str, collection_tfs: defaultdict, collection_total_terms: int) -> float:
    tf_query = tokenize(query)
    total_query_terms = sum(tf_query.values())
    return metrics.kl_divergence(tf_query, collection_tfs, total_query_terms, collection_total_terms)


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
            collection_tfs: defaultdict, collection_total_terms: int, vocabulary: list=None) -> float:
    '''
    Clarity
    :param query: query text
    :param query_result_docs_tfs: {articleId1: {'term1': count1, 'term2': count2, ...}, articleId2: {...}}
    :param collection_tfs: {'term1': count1, ...}
    :param collection_total_terms: number of terms in the collection
    :param vocabulary: all distinct terms in the collection
    :return: a float number
    '''
    query_terms = list(tokenize(query).keys())
    if vocabulary is None:
        vocabulary = query_terms

    def get_prob_t_condition_Dq(t: str, lambd=0.9) -> float:
        prob_t_condit_D = collection_tfs[t] / collection_total_terms
        prob_term_condit_doc = lambda term, tfd: lambd * (tfd[term] / sum(tfd.values())) + \
                                                         (1 - lambd) * prob_t_condit_D
        norm = 0.00000001
        for tf_d in query_result_docs_tfs.values():
            norm += reduce(lambda x, y: x*y, map(lambda x: prob_term_condit_doc(x, tf_d), query_terms))
        prob = 0.0
        for tf_d in query_result_docs_tfs.values():
            # tot_d = sum(tf_d.values())
            prob_t_condit_d = prob_term_condit_doc(t, tf_d)
            prob_q_condit_d = reduce(lambda x, y: x*y, map(lambda x: prob_term_condit_doc(x, tf_d), query_terms))
            prob_d_condit_q = prob_q_condit_d / norm
            prob += prob_t_condit_d * prob_d_condit_q
        return prob

    clt = 0.0
    for t in vocabulary:
        if collection_tfs[t] == 0:
            collection_tfs[t] = 1
        prob_t_condit_D = collection_tfs[t] / collection_total_terms
        prob_t_condit_Dq = get_prob_t_condition_Dq(t)
        clt += prob_t_condit_Dq * log(prob_t_condit_Dq / prob_t_condit_D)
    return clt


if __name__ == '__main__':
    c = config.get_paths()
    index_path = c[sys.argv[1]]
    query_file_path = sys.argv[2]
    save_path = sys.argv[3]

    config.setup_logger('querydifficulty')

    ix = index.open_dir(index_path, readonly=True)
    LOGGER.info('Index path: ' + index_path)
    ix_reader = ix.reader()

    vocabulary = []
    db_tfs = defaultdict(int)
    db_total_terms = 0
    with open(c['db_tfs'], 'r') as fr:
        for line in fr:
            parts = line.rsplit(',', 1)
            db_tfs[parts[0]] = int(parts[1])
            db_total_terms += int(parts[1])
            vocabulary.append(parts[0])

    df = pd.read_csv(query_file_path)
    df.drop_duplicates(subset='id', inplace=True)
    for q in df['query']:
        print(q)
        qdf = df[df['query'] == q]
        scs = specificity(q, db_tfs, db_total_terms)

        aids = list(map(lambda x: str(x), qdf['articleId'].values))
        docs_tfs = get_docs_tfs(aids, ix_reader)
        clt = clarity(q, docs_tfs, db_tfs, db_total_terms)

        df.loc[qdf.index, 'specificity'] = scs
        df.loc[qdf.index, 'clarity'] = clt
        print(scs, clt)
        LOGGER.info('Query: {}, specificity: {}, clarity: {}'.format(q, scs, clt))

    ix_reader.close()
    df.to_csv(save_path, index=False, columns=['id', 'query', 'specificity', 'clarity'])
