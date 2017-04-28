import os
import sys

sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
#sys.path.append('/scratch/pradap/python-work/dmagellan')
#from test_preprocess import preprocess_table
from dmagellan.blocker.attrequivalence.attrequivblocker import *

import pandas as pd
import time

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'sample_tracks.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'sample_songs.csv'), low_memory=False)
print('Reading the files done')
# C = block_tables_sm(A, B, 'ID', 'ID', 'zipcode', 'zipcode', ['zipcode'], ['zipcode'], nlchunks=1, nrchunks=2)

# D = block_candset_sm(C, A, B, "l_ID", "r_ID", "ID", "ID", "birth_year", "birth_year", nchunks=1)
# # print(len(D))
print('\nsingle machine')
start = time.time()
C = block_tables_sm(A, B, 'id', 'id', 'title', 'title', ['year'], ['year'], nlchunks=1,
                    nrchunks=2)
print('Attr. equivalence (sm): {0}'.format(time.time()-start))
print(len(C))

print('\nDask')
start = time.time()
C = block_tables_sm(A, B, 'id', 'id', 'title', 'title', ['year'], ['year'], nlchunks=1,
                    nrchunks=1)
print('Attr. equivalence (dask): {0}'.format(time.time()-start))
print(len(C))
