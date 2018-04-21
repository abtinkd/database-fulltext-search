import re, codecs


def get_article_id_from_file_name(filename):
    n = re.sub('[^0-9]', '', filename)
    if n != '' and n is not None:
        return str(n).encode('utf-8').decode('utf-8')
    else:
        return -1


# Returns file paths as a dictionary
def get():
    configs = {}
    with open('configuration.csv', 'r') as fo:
        for line in fo:
            if line[0] == '#':  # It's a comment
                continue
            c, v = line.split(';', 1)
            configs[c.strip()] = v.strip()
    return configs


def __build_wiki13_title_count():
    wiki13_title_count = {}
    configs = get()
    with codecs.open(configs['wiki13_count13'], 'r', encoding='utf-8') as fo:
        for l in fo:
            lparts = l.split(',')
            artic_id = get_article_id_from_file_name(lparts[0].rsplit('/', 1)[1].strip())
            artic_count = int(lparts[1].strip())
            artic_title = lparts[2].strip()
            wiki13_title_count[artic_id] = {'title': artic_title, 'count': artic_count}
    return  wiki13_title_count


def init():
    id_title_count = __build_wiki13_title_count()

    return id_title_count
