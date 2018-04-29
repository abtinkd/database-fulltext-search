from __future__ import division
from math import log
import logging

LOGGER = logging.getLogger()


def kl_divergence(corpus1_tfs: dict, corpus2_tfs: dict,
                  c1_total_terms: int = None, c2_total_terms: int = None) -> float:
    """Xu, Jinxi, and W. Bruce Croft. "Cluster-based language models for distributed retrieval."
    Proceedings of the 22nd annual international ACM SIGIR conference on Research and development in
    information retrieval. ACM, 1999.
    (section 2.3)"""
    if c1_total_terms is None:
        c1_total_terms = sum(corpus1_tfs.values())
    if c2_total_terms is None:
        c2_total_terms = sum(corpus2_tfs.values())
    if c1_total_terms == 0 or c2_total_terms == 0:
        LOGGER.warning('KLD 0.0000 for no-term corpus!')
        return 0.0

    kl = 0.0
    for term, c1_tf in corpus1_tfs.items():
        c2_tf = corpus2_tfs.get(term, 0)
        if c1_tf < 0 or c2_tf < 0:
            raise ValueError('Negative count for number of occurences of term {}'.format(t))
        p_t_c1 = c1_tf / c1_total_terms
        p_t_c2 = (c1_tf + c2_tf) / (c1_total_terms + c2_total_terms)
        kl += p_t_c1 + log(p_t_c1/p_t_c2)
    return kl


def avg_kl_divergence(corpus1_tfidf: dict, corpus2_tfidf: dict, vocabularies: list) -> float:
    """ Huang, Anna. "Similarity measures for text document clustering."
    Proceedings of the sixth new zealand computer science research student conference (NZCSRSC2008),
    Christchurch, New Zealand. 2008. """
    avg_kl = 0.0
    for t in vocabularies:
        sm_w_t_1 = corpus1_tfidf.get(t, 0.0) + 0.01
        sm_w_t_2 = corpus2_tfidf.get(t, 0.0) + 0.01
        pi1 = sm_w_t_1 / (sm_w_t_1 + sm_w_t_2)
        pi2 = sm_w_t_2 / (sm_w_t_1 + sm_w_t_2)
        M = pi1 * sm_w_t_1 + pi2 * sm_w_t_2
        D1 = sm_w_t_1 * log(sm_w_t_1 / M)
        D2 = sm_w_t_2 * log(sm_w_t_2 / M)
        avg_kl += pi1 * D1 + pi2 * D2
    return avg_kl
