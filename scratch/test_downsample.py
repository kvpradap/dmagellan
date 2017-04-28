import os
import sys

sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
#sys.path.append('/scratch/pradap/python-work/dmagellan')
#from test_preprocess import preprocess_table
from dmagellan.core.downsample import *
from dmagellan.utils.py_utils.utils import *

import pandas as pd
import time

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'tracks.csv'))
B = pd.read_csv(os.path.join(datapath, 'songs.csv'))
print('Reading the files done')

# stopwords
stopwords = get_stop_words(os.path.join(datapath, 'stopwords'))
stopwords.extend(['and', 'in', 'the', 'my', 'me', 'to', 'you', 'i', 'andre', 'from', 'a', 'of', 'the', 'version', 'love', 'live', 'la', 'mix', 'album', 'dont'])
stopwords = list(set(stopwords))

print('\nsingle machine')
# single machine
start = time.time()
res = downsample_sm(A, B, "id", "id", 10000, 1, stopwords=stopwords)
print('Downsample (sm): {0}'.format(time.time()-start))

# dask
print('\ndask')
start = time.time()
res = downsample_dk(A, B, "id", "id", 10000, 1, stopwords=stopwords, nlchunks=1, nrchunks=4)
print('Downsample (dask): {0}'.format(time.time()-start))

