import re, os, codecs, time, logging

# Increase limitmb & multisegment = True in large batch indexing
BUILD_limitmb = 1024
BUILD_procs = 4
BUILD_multisegment = True


def setup_logger(file_name=None):
    if not os.path.exists('log'):
        os.mkdir('log')
    logger = logging.getLogger()
    postfix = '' if file_name is None or file_name == 'root' else '_'+file_name
    hdlr = logging.FileHandler('log/{}{}.log'.format(time.strftime('%m%d_%H%M'), postfix))
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    print('Logging set up.')
    return logger


def get_article_id_from_file_name(filename):
    n = re.sub('[^0-9]', '', filename)
    if n != '' and n is not None:
        return str(n).encode('utf-8').decode('utf-8')
    else:
        return -1


# Returns file paths as a dictionary
def get_paths():
    configs = {}
    with open('configuration.csv', 'r') as fo:
        for line in fo:
            if line[0] == '#':  # It's a comment
                continue
            c, v = line.split(';', 1)
            configs[c.strip()] = v.strip()
    return configs


def build_wiki13_title_count(file_path):
    wiki13_title_count = {}
    with codecs.open(file_path, 'r', encoding='utf-8') as fo:
        for l in fo:
            lparts = l.split(',')
            artic_id = get_article_id_from_file_name(lparts[0].rsplit('/', 1)[1].strip())
            artic_count = int(lparts[1].strip())
            artic_title = lparts[2].strip()
            wiki13_title_count[artic_id] = {'title': artic_title, 'count': artic_count}
    return wiki13_title_count


