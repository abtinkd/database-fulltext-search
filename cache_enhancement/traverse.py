from __future__ import print_function
import os
import time

ERROR_LOG_FILENAME = 'failure_{}.log'.format(time.strftime('%m%d_%H%M'))


def apply(root_path, apply_func, print_process=False, log_step=1000):
    # Applies the given function on every files in the root_path
    bad_files = []
    count = [0, 0]
    tm = time.time()
    speed = 1
    for root, _, files in os.walk(root_path):
        for f in files:
            count[0] += 1
            file_path = os.path.abspath(root + '/' + f)
            if count[0] % log_step == 0:
                speed = (time.time() - tm) / float(log_step)
                tm = time.time()
            if print_process:
                print('\rfailure-rate:{:.6f}     {} | {:.5f}(s) | {}...    '.format(
                    len(bad_files) / float(count[0]), count, speed, file_path), end='')
            try:
                failed = apply_func(file_path)
                if not failed:
                    count[1] += 1
            except Exception as e:
                bad_files += [file_path]
                with open(ERROR_LOG_FILENAME, 'a') as f:
                    f.write(file_path + '  ' + str(e) + '\n')

    bd_str = '\n'.join(bad_files)
    if print_process:
        print('\nunsuccessful tries:\n{}'.format(bd_str))


# A file-path generator as it traverses the root_path
def access(root_path):
    tm = time.time()
    for root, _, files in os.walk(root_path):
        for f in files:
            file_path = os.path.abspath(root + '/' + f)
            yield f, file_path