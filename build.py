from whoosh.index import create_in, exists_in
from whoosh.fields import *
from traverse import access
from codecs import open
import logging, config
import sys, time, os

LOGGER = logging.getLogger()

schema = Schema(title=TEXT(stored=True, vector=True, analyzer=analysis.StandardAnalyzer()),
                articleID=ID(stored=True, unique=True),
                body=TEXT(stored=False, vector=True, analyzer=analysis.StandardAnalyzer()),
                count=NUMERIC(int, 32, stored=True, signed=False, sortable=True),
                xpath=STORED)


def build_index_wiki13(dir_path: str, save_path: str, count_path: str):
    wiki13_title_count = config.build_wiki13_title_count(count_path)
    if not os.path.exists(save_path):
        os.mkdir(save_path)
    if not exists_in(save_path):
        ix = create_in(save_path, schema)
    else:
        save_path = save_path + '_{}'.format(time.strftime('%m%d_%H%M'))
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        ix = create_in(save_path, schema)
    writer = ix.writer(limitmb=config.BUILD_limitmb, procs=config.BUILD_procs,
                       multisegment=config.BUILD_multisegment)
    docs = access(dir_path)
    fc = 0
    print('Building index in directory {}'.format(save_path))
    for dname, dpath in docs:
        fc +=1
        if fc%1000 == 0:
            print(fc, dpath)
        did = config.get_article_id_from_file_name(dname)
        with open(dpath, 'r', encoding='utf-8') as fo:
            dcont = fo.read()
        try:
            if did not in wiki13_title_count:
                raise LookupError('Filename \'{}\' not in title-count list'.format(did))
            writer.add_document(title= wiki13_title_count[did]['title'], articleID=did,
                                body=dcont, count=wiki13_title_count[did]['count'], xpath=dpath)
        except Exception as e:
            LOGGER.error(dpath + '  ' + str(e) + '\n')

    writer.commit()
    return


if __name__ == '__main__':
    config.setup_logger()
    c = config.get_paths()
    if len(sys.argv) >= 4:
        build_index_wiki13(c[sys.argv[1]], c[sys.argv[2]], c[sys.argv[3]])
    else:
        print('dir_alias, index_alias, count_alias is required!')

