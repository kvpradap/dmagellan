import os
import sys

sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
#sys.path.append('/scratch/pradap/python-work/dmagellan')
#from test_preprocess import preprocess_table
from dmagellan.core.attrequivalence import *

import pandas as pd
import time

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)
print('Reading the files done')
print('\nsingle machine')
start = time.time()
C = block_tables_sm(A, B, 'id', 'id', 'title', 'title', ['title'], ['title'], nlchunks=1, nrchunks=2)

D = block_candset_sm(C, A, B, "l_id", "r_id", "id", "id", "year", "year", nchunks=2)
# print(len(D))
print('Attr. equivalence (sm): {0}'.format(time.time()-start))
print(len(D))

print('\nDask')
start = time.time()
C = block_tables_dk(A, B, 'id', 'id', 'title', 'title', ['title'], ['title'], nlchunks=1, nrchunks=2)
D = block_candset_dk(C, A, B, "l_id", "r_id", "id", "id", "year", "year", nchunks=2)
print('Attr. equivalence (dask): {0}'.format(time.time()-start))
print(len(D))
