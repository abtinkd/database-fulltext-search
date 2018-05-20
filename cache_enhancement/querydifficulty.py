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


def similarity(query_terms: list, collection: IndexVirtualPartition, mode='avg') -> float:
    # mode is in {'avg', 'max', 'sum'}
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


if __name__ == '__main__':
    index_name = sys.argv[1]
    query_file_path = sys.argv[2]

    c = config.get_paths()
    ix = index.open_dir(c[index_name], readonly=True)
    LOGGER.info('Index path: ' + c[index_name])
    pd.read_csv(query_file_path)
