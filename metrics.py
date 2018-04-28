from __future__ import division
from collections import defaultdict
from math import log
import logging

LOGGER = logging.getLogger()


def kl_divergence(corpus1_tfs: defaultdict, corpus2_tfs: defaultdict, vocab_size: int,
                  corpus1_total_terms: int=None, corpus2_total_terms: int=None) -> float:
    """Xu, Jinxi, and W. Bruce Croft. "Cluster-based language models for distributed retrieval."
    Proceedings of the 22nd annual international ACM SIGIR conference on Research and development in
    information retrieval. ACM, 1999."""
    if corpus1_total_terms is None:
        corpus1_total_terms = sum(corpus1_tfs.values())
    if corpus2_total_terms is None:
        corpus2_total_terms = sum(corpus2_tfs.values())
    if corpus1_total_terms == 0 or corpus2_total_terms == 0:
        LOGGER.warning('KLD 0.0000 for no-term corpus!')
        return 0.0

    kl = 0.0
    for t, f in corpus1_tfs.items():
        if f < 0 or corpus2_tfs[t] < 0:
            raise ValueError('Negative count for number of occurences of term {}'.format(t))
        # added 0.01 to nominator and denominator of p_t_c1 to make kl divergence of two identical corpus 1.0
        p_t_c1 = (f+0.01)/(corpus1_total_terms+0.01*vocab_size)
        p_t_c2 = (corpus2_tfs[t]+0.01)/(corpus2_total_terms+0.01*vocab_size)
        kl += p_t_c1 + log(p_t_c1/p_t_c2)
    return kl


def avg_kl_divergence(corpus1_tfidf: defaultdict, corpus2_tfidf: defaultdict, vocabularies: set) -> float:
    """ Huang, Anna. "Similarity measures for text document clustering."
    Proceedings of the sixth new zealand computer science research student conference (NZCSRSC2008),
    Christchurch, New Zealand. 2008. """
    avg_kl = 0.0
    for t in list(vocabularies):
        sm_w_t_1 = corpus1_tfidf[t] + 0.01
        sm_w_t_2 = corpus2_tfidf[t] + 0.01
        pi1 = sm_w_t_1[t] / (sm_w_t_1[t] + sm_w_t_2[t])
        pi2 = sm_w_t_2[t] / (sm_w_t_1[t] + sm_w_t_2[t])
        M = pi1 * sm_w_t_1[t] + pi2 * sm_w_t_2[t]
        D1 = sm_w_t_1[t] * log(sm_w_t_1[t] / M)
        D2 = sm_w_t_2[t] * log(sm_w_t_2[t] / M)
        avg_kl += pi1 * D1 + pi2 * D2
    return avg_kl
