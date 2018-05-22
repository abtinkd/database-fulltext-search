import pandas as pd
import sys
from collections import defaultdict
import config
from partition import IndexVirtualPartition, Partitioner
import whoosh.index as index
import whoosh.analysis as analysis
from math import log
from whoosh.reading import IndexReader
import logging
from functools import reduce

LOGGER = logging.getLogger()


def tokenize(query: str) -> defaultdict:
    analyzer = analysis.StandardAnalyzer()
    tokens = defaultdict(int)
    for token in analyzer(query):
        tokens[token.text] += 1
    return tokens


def specificity(tf_query: defaultdict, total_query_terms: int,
                tf_collection: defaultdict, total_collection_terms: int) -> float:
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


def clarity(query: str, query_results_docnum: list, collection: IndexVirtualPartition, fieldname='body'):
    query_terms = list(tokenize(query).keys())
    collection_tfs = collection.get_tfs()
    collection_total_terms = collection.get_total_terms()
    vocabulary = list(collection_tfs.keys())

    def get_prob_t_condition_Dq(t: str, lambd=0.9) -> float:
        prob_t_condit_D = collection_tfs[t] / collection_total_terms
        norm = 0.0
        for d in query_results_docnum:
            norm += reduce(lambda tm, y: (tf_d[tm]/tot_d)*y, query_terms)
        prob = 0.0
        for dn in query_results_docnum:
            if collection._reader.has_vector(dn, fieldname):
                tf_d = dict(collection._reader.vector_as('frequency', dn, fieldname))
                tot_d = sum(tf_d.values())
                prob_t_condit_d = lambd * (tf_d[t]/tot_d) + (1-lambd) * prob_t_condit_D
                prob_q_condit_d = reduce(lambda tm, y: (tf_d[tm]/tot_d)*y, query_terms)
                prob_d_condit_q = prob_q_condit_d / norm
                prob += prob_t_condit_d * prob_d_condit_q
            else:
                LOGGER.warning('No forward vector was found for query {} on docnum {}'.format(query, dn))
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

    c = config.get_paths()
    ix = index.open_dir(c[index_name], readonly=True)
    LOGGER.info('Index path: ' + c[index_name])
    pd.read_csv(query_file_path)
