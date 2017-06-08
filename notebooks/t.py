import pandas as pd
from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching
from distributed import Client
from dask.multiprocessing import get
import psutil
A = pd.read_csv('./chtc/data/sample_citeseer.csv', usecols=['id', 'title'])
B = pd.read_csv('./chtc/data/sample_dblp.csv', usecols=['id', 'title'])
print('Starting to read files (mem: {0})' , str(psutil.virtual_memory().used/1e9))
orig_A = pd.read_csv('./chtc/data/citeseer_nonans.csv', usecols=['id', 'title'])
orig_B = pd.read_csv('./chtc/data/dblp_nonans.csv', usecols=['id', 'title'])
print('Read files done !!!(mem: {0})', psutil.virtual_memory().used/1e9)

ob = OverlapBlocker()
C = ob.block_tables(A.sample(1000), B.sample(1000), 'id', 'id', 'title', 'title', overlap_size=6, nltable_chunks=2, nrtable_chunks=2, rem_stop_words=True)
client = Client('127.0.0.1:8786')
D = C.compute(get=client.get)
