# Database-Fulltext-Search
Improving the effectiveness of keyword search on relational data using effective retrieval subsets.

# Project Structure:
* [Cache Enhancement](https://github.com/abtkod/database-fulltext-search/tree/master/cache_enhancement): This project gives the capability of indexing all documents in a directory and its subdirectories, filtering the parts of the generated index that satisfy a specific condition, and measuring query difficulty metrics against the selected part of the index. Main modules are:
  * [build.py](https://github.com/abtkod/database-fulltext-search/blob/master/cache_enhancement/build.py): used to build an index based on a directory of documents;
  * [partition.py](https://github.com/abtkod/database-fulltext-search/blob/master/cache_enhancement/partition.py): used to build a virtual partition on top of the main index;
  * [querydifficulty.py](https://github.com/abtkod/database-fulltext-search/blob/master/cache_enhancement/querydifficulty.py): our main query difficulty metrics;
  * [enhancer/describe.py](https://github.com/abtkod/database-fulltext-search/blob/master/cache_enhancement/enhancer/describe.py): used to compare two different virtual partitions, treating them as two giant documents;
  * [enhancer/solutions.py](https://github.com/abtkod/database-fulltext-search/blob/master/cache_enhancement/enhancer/solutions.py): used to recursively refine a virtual partition by removing documents from it to increase its difference from another base partition.
* [MSLR](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/MSLR.ipynb): applies Learn-to-rank on the MSLR dataset;
* [rrank-analysis](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/rrank-analysis.ipynb): effect of cache size on the reciprocal ranks;
* [ML-evaluate](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/ml_evaluate_models.ipynb): evaluates machine learninng models as the cache selection algorithm; 
* [ML-prepare](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/ml_prepare_data.ipynb)
* [Cluster Analysis](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/cluster-analysis.ipynb)
* [text-classification](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/text_classification.ipynb)
* [wiki13](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/wiki13.ipynb)
* [wikipagecount](https://nbviewer.jupyter.org/github/abtkod/database-fulltext-search/blob/master/data_analysis/wiki-page-count.ipynb) 
