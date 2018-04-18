import re, codecs


def get_article_id_from_file_name(filename):
    n = re.sub('[^0-9]', '', filename)
    if n != '' and n != None:
        return str(n).encode('utf-8').decode('utf-8')
    else:
        return -1


# Returns file paths as a dictionary
def get():
    configs = {}
    with open('configuration.csv', 'r') as fo:
        for line in fo:
            c,v = line.split(';', 1)
            configs[c.strip()] = v.strip()
    return configs


def build_wiki13_title_count(configs):
    wiki13_title_count = {}
    with codecs.open(configs['wiki13_count13'], 'r', encoding='utf-8') as fo:
        for l in fo:
            lparts = l.split(',')
            artic_id = get_article_id_from_file_name(lparts[0].rsplit('/', 1)[1].strip())
            artic_count = int(lparts[1].strip())
            artic_title = lparts[2].strip()
            wiki13_title_count[artic_id] = {'title': artic_title, 'count': artic_count}
    return  wiki13_title_count


def init():
    configs = get()
    wiki13_title_count = build_wiki13_title_count(configs)

    return wiki13_title_count
