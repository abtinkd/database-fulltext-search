from whoosh import index
import config, time

cp = config.get_paths()
ix = index.open_dir(cp['sample_index'])

with ix.reader() as r:
    ids = r.all_doc_ids()
    rids = {}
    for id in ids:
        if not r.has_vector(id, 'body'):
            sf = r.stored_fields(id)
            rids[id] = sf

with ix.writer() as w:
    for id in rids.keys():
        w.delete_document(id)


with open('log/cleaned_{}.log'.format(time.strftime('%m%d_%H%M')), 'w') as w:
    for v in rids.items():
        w.write('{},{},{},{},{}\n'.format(v[0], v[1]['articleID'], v[1]['count'],v[1]['title'], v[1]['xpath']))
