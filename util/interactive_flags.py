#!python3

import sys, os

pace_util_path = os.path.abspath('..')
if pace_util_path not in sys.path:
    sys.path.append(pace_util_path)

from pace_util import fileflag, clear_fileflag, set_fileflag

if __name__ == '__main__':
    try:
        set_fileflag(sys.argv[1])
        print('\nSET FILEFLAG ' + sys.argv[1])
    except IndexError:
        import pdb; pdb.set_trace()