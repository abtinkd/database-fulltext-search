import whoosh.index as index
from whoosh.qparser import QueryParser
import config
import sys

def search(user_query: str, index_dir_name):
    c = config.get()
    ix = index.open_dir(c[index_dir_name])
    print('Results for query [{}] in directory [{}]'.format(user_query, c[index_dir_name]))
    with ix.searcher() as searcher:
        query = QueryParser("body", ix.schema).parse(user_query)
        results = searcher.search(query)
        print(results)
        for res in results:
            print(res)


if __name__ == '__main__':
    user_query = 'link'
    if len(sys.argv) > 1:
        user_query = ' '.join(sys.argv[1:])

    search(user_query, 'wiki13_index')