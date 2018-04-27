import whoosh.index as index
from whoosh.qparser import QueryParser
from whoosh import sorting
import config
import sys


def search(user_query: str, limit: int, index_dir_path: str, field_name="body"):
    ix = index.open_dir(index_dir_path, readonly=True)
    print('Results for query [{}] in directory [{}]'.format(user_query, index_dir_path))
    with ix.searcher() as searcher:
        query = QueryParser(field_name, ix.schema).parse(user_query)
        facet = sorting.FieldFacet('count', reverse=True)
        results = searcher.search(query, sortedby=facet, limit=limit)
        print(results)
        for res in results:
            print('\n', res)
            if res.reader.has_vector(res.docnum, field_name):
                vgen = res.reader.vector_as('frequency', res.docnum, field_name)
                terms = [v for v in vgen]
                terms.sort(key=lambda tup: tup[1], reverse=True)
                print('Top terms: ', terms)
            else:
                print('0 term')


if __name__ == '__main__':
    index_name = 'wiki13_index'
    limit = None
    if len(sys.argv) > 1:
        index_name = sys.argv[1]
    if len(sys.argv) > 2:
        limit = sys.argv[2]
    configuration = config.get_paths()

    user_query = 'public policy NOT \"public policy\"'
    while user_query != ':q':
        search(user_query, limit, configuration[index_name])
        user_query = input('Query [:q to exit] : ')
