import pandas as pd
from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching
from distributed import Client
from dask.multiprocessing import get
A = pd.read_csv('./chtc/data/sample_citeseer.csv', usecols=['id', 'title'])
B = pd.read_csv('./chtc/data/sample_dblp.csv', usecols=['id', 'title'])
orig_A = pd.read_csv('./chtc/data/citeseer_nonans.csv', usecols=['id', 'title'])
orig_B = pd.read_csv('./chtc/data/dblp_nonans.csv', usecols=['id', 'title'])

F = get_features_for_matching(A, B)
L = pd.read_csv('./chtc/data/sample_labeled_data.csv')
# Convert L into feature vectors using updated F
H = extract_feature_vecs(L, orig_A, orig_B,
                         '_id', 'l_id', 'r_id', 'id', 'id',
                          feature_table=F,
                          attrs_after='label', nchunks=4,
                          show_progress=True, compute=False,
                         scheduler=get)

client = Client('127.0.0.1:8786')
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
# with Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
p = H.compute(get=client.get)
print('Hello')
