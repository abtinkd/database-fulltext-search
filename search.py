from whoosh.fields import *
import whoosh.index as index


ix = index.open_dir('data')
from whoosh.qparser import QueryParser
with ix.searcher() as searcher:
    query = QueryParser("content", ix.schema).parse("rate")
    results = searcher.search(query)
    for r in results:
        print(r)