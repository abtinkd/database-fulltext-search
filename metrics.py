from collections import defaultdict
from math import log


def kl_divergence(corpus1_tfs: defaultdict, corpus2_tfs: defaultdict) -> float:
    kl = 0.0
    c1_terms_count = sum(corpus1_tfs.values())
    c2_terms_count = sum(corpus2_tfs.values())
    for t, f in corpus1_tfs.items():
        if corpus2_tfs[t] <= 0:
            continue
        kl += f/(c1_terms_count*1.0) + log(corpus2_tfs[t]/(c2_terms_count*1.0))
    return kl


def avg_kl_divergence(corpus1_tfs: defaultdict, corpus2_tfs: defaultdict) -> float:
    return 1.0
