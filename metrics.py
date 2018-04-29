from __future__ import division
from math import log
import logging

LOGGER = logging.getLogger()


def kl_divergence(corpus1_measure: dict, corpus2_measure: dict,
                  corpus1_normalization_factor=1.0, corpus2_normalization_factor=1.0) -> float:
    """Xu, Jinxi, and W. Bruce Croft. "Cluster-based language models for distributed retrieval."
    Proceedings of the 22nd annual international ACM SIGIR conference on Research and development in
    information retrieval. ACM, 1999.
    (section 2.3)"""

    if corpus1_normalization_factor == 0 or corpus2_normalization_factor == 0:
        LOGGER.warning('KLD 0.0000 for no-term corpus!')
        return 0.0

    kl = 0.0
    for t, sm_w_t_1 in corpus1_measure.items():
        sm_w_t_2 = corpus2_measure.get(t, 0.0)
        if sm_w_t_1 < 0 or sm_w_t_2 < 0:
            raise ValueError('Negative count for number of occurences of term {}'.format(t))
        p_t_c1 = sm_w_t_1 / corpus1_normalization_factor
        p_t_c2 = (sm_w_t_1 + sm_w_t_2) / (corpus1_normalization_factor + corpus2_normalization_factor)
        kl += p_t_c1 * log(p_t_c1 / p_t_c2)
    return kl


def avg_kl_divergence(corpus1_measure: dict, corpus2_measure: dict,
                      corpus1_normalization_factor=1.0, corpus2_normalization_factor=1.0) -> float:
    """ Huang, Anna. "Similarity measures for text document clustering."
    Proceedings of the sixth new zealand computer science research student conference (NZCSRSC2008),
    Christchurch, New Zealand. 2008. """
    # vocabularies = list(set(corpus1_tfidf.keys()).union(set(corpus2_tfidf.keys())))
    # one way comparison for simplicity
    vocabularies = corpus1_measure if len(corpus1_measure) <= len(corpus2_measure) else corpus2_measure
    avg_kl = 0.0
    for t in vocabularies:
        sm_w_t_1 = (corpus1_measure.get(t, 0.0) / corpus1_normalization_factor) + 0.01
        sm_w_t_2 = (corpus2_measure.get(t, 0.0) / corpus2_normalization_factor) + 0.01
        pi1 = sm_w_t_1 / (sm_w_t_1 + sm_w_t_2)
        pi2 = sm_w_t_2 / (sm_w_t_1 + sm_w_t_2)
        M = pi1 * sm_w_t_1 + pi2 * sm_w_t_2
        D1 = sm_w_t_1 * log(sm_w_t_1 / M)
        D2 = sm_w_t_2 * log(sm_w_t_2 / M)
        avg_kl += pi1 * D1 + pi2 * D2
    return avg_kl
