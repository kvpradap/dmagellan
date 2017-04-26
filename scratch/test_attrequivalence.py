import sys
import os
sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
#sys.path.append('/scratch/pradap/python-work/dmagellan')
#from test_preprocess import preprocess_table
from dmagellan.core.attrequivalence import *
from dmagellan.core.utils import *

import pandas as pd
import time

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)
print('Reading the files done')

print('\nsingle machine')
start = time.time()
C = block_tables_sm(A, B, 'id', 'id', 'title', 'title', ['title'], ['title'], nlchunks=1,
                    nrchunks=2)
print('Attr. equivalence (sm): {0}'.format(time.time()-start))
print(len(C))

print('\nDask')
start = time.time()
C = block_tables_sm(A, B, 'id', 'id', 'title', 'title', ['title'], ['title'], nlchunks=1,
                    nrchunks=1)
print('Attr. equivalence (dask): {0}'.format(time.time()-start))
print(len(C))

