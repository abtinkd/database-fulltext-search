from __future__ import division
from collections import defaultdict
from math import log


'''Xu, Jinxi, and W. Bruce Croft. "Cluster-based language models for distributed retrieval." 
Proceedings of the 22nd annual international ACM SIGIR conference on Research and development in information retrieval. 
ACM, 1999.'''
def kl_divergence(corpus1_tfs: defaultdict, corpus2_tfs: defaultdict) -> float:
    kl = 0.0
    c1_terms_count = sum(corpus1_tfs.values())
    c2_terms_count = sum(corpus2_tfs.values())
    for t, f in corpus1_tfs.items():
        if corpus2_tfs[t] <= 0 or corpus1_tfs[t] <= 0:
            continue
        p_t_c1 = f/c1_terms_count
        p_t_c2 = corpus2_tfs[t]/c2_terms_count
        kl += p_t_c1 + log(p_t_c1/p_t_c2)
    return kl


'''Huang, Anna. "Similarity measures for text document clustering." 
Proceedings of the sixth new zealand computer science research student conference (NZCSRSC2008), 
Christchurch, New Zealand. 2008.'''
def avg_kl_divergence(corpus1_tfidf: defaultdict, corpus2_tfidf: defaultdict) -> float:
    vocab = set(corpus1_tfidf.keys())
    vocab.update(set(corpus2_tfidf.keys()))
    avg_kl = 0.0
    for t in list(vocab):
        sm_w_t_1 = corpus1_tfidf[t] + 0.01
        sm_w_t_2 = corpus2_tfidf[t] + 0.01
        pi1 = sm_w_t_1[t] / (sm_w_t_1[t] + sm_w_t_2[t])
        pi2 = sm_w_t_2[t] / (sm_w_t_1[t] + sm_w_t_2[t])
        M = pi1 * sm_w_t_1[t] + pi2 * sm_w_t_2[t]
        D1 = sm_w_t_1[t] * log(sm_w_t_1[t] / M)
        D2 = sm_w_t_2[t] * log(sm_w_t_2[t] / M)
        avg_kl += pi1 * D1 + pi2 * D2
    return avg_kl
