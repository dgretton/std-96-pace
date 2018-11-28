#!python3

import sys, os

this_file_dir = os.path.dirname(__file__)
pyham_methods_dir = os.path.abspath(os.path.join(this_file_dir, '..'))

basic_pace_mod_path = os.path.join(pyham_methods_dir, 'basic_pace')
if basic_pace_mod_path not in sys.path:
    sys.path.append(basic_pace_mod_path)

from basic_pace_181024 import fileflag, clear_fileflag, set_fileflag

if __name__ == '__main__':
    try:
        set_fileflag(sys.argv[1])
        print('\nSET FILEFLAG ' + sys.argv[1])
    except IndexError:
        import pdb; pdb.set_trace()