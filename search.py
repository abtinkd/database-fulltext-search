import whoosh.index as index
from whoosh.qparser import QueryParser
import config
import sys


def search(user_query: str, index_dir_path):
    ix = index.open_dir(index_dir_path)
    print('Results for query [{}] in directory [{}]'.format(user_query, index_dir_path))
    with ix.searcher() as searcher:
        query = QueryParser("body", ix.schema).parse(user_query)
        results = searcher.search(query)
        print(results)
        for res in results:
            print(res)


if __name__ == '__main__':
    user_query = 'public policy NOT \"public policy\"'
    configuration = config.get()
    if len(sys.argv) > 1:
        user_query = ' '.join(sys.argv[1:])

    search(user_query, configuration['wiki13_index'])
