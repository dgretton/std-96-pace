#!python3

import sys, os

pace_util_path = os.path.abspath('..')
if pace_util_path not in sys.path:
    sys.path.append(pace_util_path)

from pace_util import HamiltonInterface, initialize, wash_empty_refill, LayoutManager, LAYFILE

LayoutManager(LAYFILE, install=True) # Install layout file with washer

if __name__ == '__main__':
    print('This will run forever until you stop it.')
    with HamiltonInterface() as ham_int:
        initialize(ham_int)
        while(True):
            wash_empty_refill(ham_int, refillAfterEmpty=2, # 2=Refill chamber 1 only
                              chamber1WashLiquid=1)        # 1=liquid 2 (blue container) (water)