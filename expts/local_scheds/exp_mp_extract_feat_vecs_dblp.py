import pandas as pd
import time
import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')

from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching

from dask import multiprocessing, threaded
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle
filename='./profres_exp_mt_dblp_300k_extractfeatvecs.html'

pbar = ProgressBar()
pbar.register()

#print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('./datasets/sample_citeseer_300k.csv')
B = pd.read_csv('./datasets/sample_dblp_300k.csv')
#print("Mem. usage after reading:{0}".format(psutil.virtual_memory().used/1e9))

C = pd.read_csv('./datasets/candset.csv')

feature_table = get_features_for_matching(A, B)

feature_vecs = extract_feature_vecs(C, A, B, '_id', 'l_id',  'r_id', 'id', 'id', feature_table=feature_table,
        nchunks=4, compute=False)

with Profiler() as prof, CacheProfiler() as cprof, ResourceProfiler(dt=0.25) as rprof:
    D = feature_vecs.compute(get=threaded.get, num_workers=4)


visualize([prof, cprof, rprof], file_path=filename, show=False)
