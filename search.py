from whoosh.fields import *
import whoosh.index as index


ix = index.open_dir('/data/khodadaa/index_wiki13')
from whoosh.qparser import QueryParser
with ix.searcher() as searcher:
    query = QueryParser("content", ix.schema).parse("statistical")
    results = searcher.search(query)
    for r in results:
        print(r)
