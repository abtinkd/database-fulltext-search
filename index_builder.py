from whoosh.index import create_in
from whoosh.fields import *
from traverse import access
from codecs import open
import config
import sys, time

ERROR_LOG_FILENAME = 'errors-build-index_{}.log'.format(time.strftime('%m%d_%H%M'))

schema = Schema(title=KEYWORD(stored=True), articleID=ID(stored=True),
                body=TEXT(analyzer=analysis.StemmingAnalyzer()), count=NUMERIC(stored=True), xpath=STORED)


def build_index_wiki13(dir_path: str, save_path: str):
    wiki13_title_count = config.init()
    ix = create_in(save_path, schema)
    writer = ix.writer()
    docs = access(dir_path)
    fc = 0
    for dname, dpath in docs:
        fc +=1
        if fc%1000 == 0:
            print(fc, dpath)
        did = config.get_article_id_from_file_name(dname)
        try:
            if did not in wiki13_title_count:
                raise LookupError('Filename \'{}\' not in title-count list'.format(did))
            with open(dpath, 'r', encoding='utf-8') as fo:
                dcont = fo.read()
            writer.add_document(title= wiki13_title_count[did]['title'], articleID=did,
                                body=dcont, count=wiki13_title_count[did]['count'], xpath=dpath)
        except Exception as e:
            with open(ERROR_LOG_FILENAME, 'a') as fw:
                fw.write(dpath + '  ' + str(e) + '\n')
    writer.commit()
    return


if __name__ == '__main__':
    c = config.get()

    if len(sys.argv) > 3:
        build_index_wiki13(sys.argv[1], sys.argv[2])
    else:
        build_index_wiki13(c['wiki13_dir'], c['wiki13_index'])
